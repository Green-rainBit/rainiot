# NATS 异步回调方案设计

## 1. 背景

### 当前 NATS 路径

```
设备 ──▶ iotws ── NATS Publish ──▶ iotdevice ──▶ DB
  ◀── (nil, false, nil)
```

NATS 是即发即弃（fire-and-forget），设备不收到处理结果。对于 `report` 类消息没问题，但对于需要结果的场景（配置下发、OTA 指令、远程重启）缺少回调机制。

### 目标

1. 支持需要响应的消息（设备侧 `needReply: true`）
2. 不需要响应的消息走现有即发即弃路径（零额外开销）
3. 支持 100K+ 连接，资源消耗可控

---

## 2. 方案选型

### 2.1 每连接一个回复订阅（否决）

```
每个 WebSocket 连接一个 NATS 订阅 "iotws.reply.{connId}"
100K 连接 = 100K 订阅 → NATS server OOM ❌
```

### 2.2 共享回复通道 + connId 分发（采用）

```
所有设备共用 1 个回复订阅 "iotws.reply"
iotws 按 ConnId 元数据分发到对应 WebSocket 连接
```

|                  | 每连接订阅 | 共享订阅                 |
| ---------------- | ---------- | ------------------------ |
| iotws 订阅数     | 100K       | **1**              |
| NATS server 内存 | ~100MB     | **~1KB**           |
| 分发延迟         | NATS 内部  | +1 次 map.Lookup (~50ns) |

---

## 3. 方案设计

### 3.1 整体架构

```
设备-A          设备-B           iotws                          NATS                    iotdevice
  │               │                │                             │                        │
  │── cmd ───────▶│                │                             │                        │
  │  needReply=true               │                             │                        │
  │  seq=123      │                │                             │                        │
  │               │── cmd ────────▶│─ Publish("iot_device", ───▶│─ consumer ────────────▶│
  │               │                │   ConnId=device-B,          │                        │ process
  │               │                │   ReplySub=iotws.reply)     │                        │
  │               │                │                             │                        │
  │               │                │              Publish("iotws.reply", ◀────────────────│
  │               │                │                ConnId=device-B,                       │
  │               │                │                Payload=result)                       │
  │               │                │◀─────────────────────────────────────────────────────│
  │               │                │                             │
  │               │                ├─ read ConnId metadata       │
  │               │                ├─ connTable.Lookup("device-B") → socket
  │               │◀── result ────┤
  │               │                └─ socket.WriteMessage(result)
```

### 3.2 消息格式

设备侧（不变，加两个可选字段）：

```json
// 不需要回复（现有格式，完全兼容）
{"cmd": "heartbeat", "sn": "device-001", "data": {"ts": 123456}}

// 需要回复
{"cmd": "control", "sn": "device-001", "seq": 42, "needReply": true, "data": {...}}
```

iotws 发布时在 Metadata 携带回复信息：

```
watermill.Message.Metadata:
  ConnId       = "device-001"
  ServiceName  = "iotws-api"
  ReplySubject = "iotws.reply"     ← 固定值，共享通道
```

iotdevice 回复时：

```
watermill.Message.Metadata:
  ConnId = "device-001"            ← 原样带回
```

### 3.3 iotws 回复分发协程

```go
// servicecontext.go 或 main.go 中启动
func startReplyWorker(mq queue.Queue, connTable ws.Connection) {
    replyCh, err := mq.Subscribe(context.Background(), "iotws.reply")
    if err != nil {
        log.Fatalf("reply subscribe failed: %v", err)
    }

    workers := runtime.GOMAXPROCS(0) * 2  // CPU 核数 × 2
    for i := 0; i < workers; i++ {
        go func() {
            for msg := range replyCh {
                connId := msg.Metadata.Get("ConnId")
                if connId == "" {
                    msg.Ack()
                    continue
                }
                // 查连接表找到对应 WebSocket，写回
                socket, ok := connTable.Load(connId)
                if !ok {
                    msg.Ack()
                    continue  // 设备已离线，丢弃
                }
                socket.WriteMessage(gws.OpcodeText, msg.Payload)
                msg.Ack()
            }
        }()
    }
}
```

### 3.4 iotws 发布时决定是否带回复地址

```go
// iotwslogic.go
func (l *IotwsLogic) Iotws(connId string, message []byte) ([]byte, bool, error) {
    if connId == "" {
        return l.loginCli.Push(l.ctx, connId, message)  // 登录不变
    }

    // 检查是否需要回复
    if extractNeedReply(message) {
        // 带 ReplySubject 发布
        return l.messageCli.PushWithReply(l.ctx, connId, message, replySubject)
    }
    // 不需要回复：现有即发即弃路径
    return l.messageCli.Push(l.ctx, connId, message)
}

func extractNeedReply(payload []byte) bool {
    return bytes.Contains(payload, []byte(`"needReply":true`))
}
```

### 3.5 iotdevice 处理完投递回复

```go
// routes.go processMessage
func processMessage(cmd string, svcCtx *svc.ServiceContext, msg *message.Message) error {
    // ... 现有逻辑 ...

    result, err := logic.NewIotdeviceLogic(msg.Context(), cmd, svcCtx).Iotdevice(req)
    if err != nil {
        return err
    }

    // 如果需要回复
    if replySub := msg.Metadata.Get("ReplySubject"); replySub != "" {
        replyPayload, _ := protojson.Marshal(result)
        reply := message.NewMessage(watermill.NewUUID(), replyPayload)
        reply.Metadata.Set("ConnId", msg.Metadata.Get("ConnId"))
        svcCtx.Queue.Publish(replySub, reply)
    }
    return nil
}
```

---

## 4. 资源消耗分析

### 4.1 共享回复订阅

| 资源             | 消耗                                                            |
| ---------------- | --------------------------------------------------------------- |
| NATS 订阅数      | 1（所有连接共享）                                               |
| NATS server 内存 | <1KB                                                            |
| iotws goroutine  | `GOMAXPROCS × 2`（~16 个）                                   |
| 分发开销         | 1 次`connTable.Load(connId)` = 1 次 `sync.Map.Load` = ~50ns |

### 4.2 回复链路延迟

```
iotdevice Publish → NATS 转发 → iotws replyCh → map.Load → socket.WriteMessage
                                                                     │
                                              本地约 0.1ms ──────────┘
                                              跨节点约 1-3ms（取决于 NATS 延迟）
```

### 4.3 容量估算

| 场景             | 容量                                                |
| ---------------- | --------------------------------------------------- |
| 回复消息速率     | 受 NATS 单主题吞吐限制（~100K msg/s）               |
| replyCh 队列深度 | 由`*nats.Subscriber` 的 `SubscribersCount` 控制 |
| 离线设备回复     | `connTable.Load` 返回 nil → 直接丢弃（无泄漏）   |

---

## 5. 消费端隔离

对于带 `needReply` 的慢消息（如 OTA），iotdevice consumer 用信号量控制并发，避免慢回复拖垮快回复：

```go
// routes.go: 可配置的 cmd 并发限制
var cmdConcurrency = map[string]int{
    "control": 64,   // 控制指令
    "ota":     8,    // OTA 升级，极慢
}
```

---

## 6. 改动清单

| 文件                      | 改动                                                 | 风险 |
| ------------------------- | ---------------------------------------------------- | ---- |
| `iotwslogic.go`         | 新增`extractNeedReply`，按需选 Push/PushWithReply  | 低   |
| `mqClient.go`           | 新增`PushWithReply` 方法（仅多一个 metadata 字段） | 低   |
| `servicecontext.go`     | 新增`startReplyWorker` 回复分发协程                | 低   |
| `connection.go`         | ConnInfo 结构体（可选，封装连接元数据）              | 低   |
| `routes.go` (iotdevice) | `processMessage` 加回复投递逻辑                    | 低   |

## 7. 兼容性

| 场景                                            | 行为                               |
| ----------------------------------------------- | ---------------------------------- |
| 现有`{"cmd":"login",...}`（无 `needReply`） | 走即发即弃，行为不变 ✅            |
| 新`{"cmd":"control",...,"needReply":true}`    | 走带回复路径，收到处理结果 ✅      |
| iotdevice 升级前（不支持 ReplySubject）         | 忽略 metadata，行为不变 ✅         |
| iotws 升级前（无 replyWorker）                  | 回复投递到无人订阅的主题，过期丢弃 |

## 8. 结论

- 1 个共享订阅替代 100K 个独立订阅，资源消耗可忽略
- 不需要回复的消息走现有路径，零额外开销
- 改动 5 个文件，全部向后兼容
