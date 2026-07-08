# 并发登录方案设计

## 1. 背景

### 当前状态

`feature/greenrainbit-performance_optimization` 分支已完成以下优化：

- NATS stream 预创建，发布端关闭 `autoProvision`，消除每次 publish 的 `StreamInfo` API 调用
- `DeviceCli.Push` 接口改为三返回值 `([]byte, bool, error)`，NATS 返回 `shouldRespond=false`
- WebSocket Gateway 在 `shouldRespond=false` 时跳过空帧写
- iotdevice NATS 消费者改为直接路由（消除二次发布）
- `ParallelGolimit` 配置化（默认 1024）
- `logx.Logger` 统一日志

### 当前登录流程

```
WS connect?connId=device-001 ──▶ 握手时注册 connId
  后续消息:
    Gateway.OnMessage → IotwsLogic → deviceCli.Push → NATS
    → (nil, false, nil) → 不写响应
```

**问题**：如果客户端没在 URL 传 `connId`，第一条 login 消息走 `Fn("", msg)`：

```
case false:
    by, _, err := c.Fn("", message.Bytes())  // NATS: (nil, false, nil)
    sn := c.extractSn(by)                    // nil → sn = ""
    // connId 永远注册不上
```

### 目标

1. 登录走 HTTP/gRPC 同步，获取认证结果和 connId
2. 已登录的消息走 NATS 异步（高性能）
3. 支持百万设备并发上线

---

## 2. 方案设计

### 2.1 双链架构

```
                 ┌── connId 为空? ──yes──▶ loginCli.Push (HTTP/gRPC 同步)
                 │                            │
  DeviceCli ────┤                             ├─ 返回 {"connId":"device-001",...}
                 │                            ├─ extractSn → 注册 session
                 │                            └─ socket.WriteMessage(resp)
                 │
                 └── connId 非空 ──────▶ messageCli.Push (NATS 异步)
                                              │
                                              └─ (nil, false, nil) → 不写响应
```

### 2.2 配置

```json
{
  "LoginTransportModel": "http,grpc",
  "MessageTransportModel": "nats"
}
```

| 配置项 | 值 | 说明 |
|--------|-----|------|
| `LoginTransportModel` | `"http,grpc"` | 登录走 HTTP 优先，gRPC 兜底 |
| `MessageTransportModel` | `"nats"` | 已认证消息走 NATS 异步 |

### 2.3 数据结构变更

```go
// servicecontext.go
type ServiceContext struct {
    LoginCli   devicecli.DeviceCli  // HTTP/gRPC，仅用于登录
    MessageCli devicecli.DeviceCli  // NATS，仅用于已认证消息
    // ... 其他字段不变
}

// iotwslogic.go
type IotwsLogic struct {
    loginCli   devicecli.DeviceCli
    messageCli devicecli.DeviceCli
}

func (l *IotwsLogic) Iotws(connId string, message []byte) ([]byte, bool, error) {
    if connId == "" {
        // 登录：同步，需要响应
        resp, shouldRespond, err := l.loginCli.Push(l.ctx, connId, message)
        return resp, true, err
    }
    // 已认证消息：异步，不写响应
    _, shouldRespond, err := l.messageCli.Push(l.ctx, connId, message)
    return nil, false, err
}
```

### 2.4 Gateway 不变

Gateway 逻辑与当前一致，不需要修改：

- `connId == ""` → `Fn` 返回 `by`（登录响应含 connId）→ `extractSn(by)` → 注册 session
- `connId != ""` → `Fn` 返回 `shouldRespond=false` → 跳过写响应

---

## 3. 并发登录容量分析

### 3.1 单次登录开销

| 操作 | 耗时 |
|------|------|
| Redis GET (检查连接状态) | ~0.5ms |
| Redis SISMEMBER (设备缓存) | ~0.5ms |
| Redis SET (记录连接) | ~0.5ms |
| 序列化/网络 | ~1ms |
| **合计** | **~2.5ms** |

> Postgres SELECT 被 Redis 缓存替代（见 3.3），不计入热路径。

### 3.2 并发容量估算

```
go-zero HTTP server: 默认 10000 并发连接
10000 / 0.0025s = 4,000,000 登录/秒 (CPU 理论上限)

实际瓶颈：Redis cluster
3 ops × 节点并发 / 0.0025s ≈ 取决于集群规模
单节点 ~5万 ops/s → 支撑 ~16,000 登录/秒

100 万设备 60 秒上线: 16,667 登录/秒
→ 单 Redis 节点刚好够用，3 节点 cluster 充分冗余
```

### 3.3 设备缓存（防 DB 瓶颈）

```go
// iotloginlogic.go: 设备存在性查询用 Redis Set 缓存
func (l *iotLoginLogic) checkDeviceExists(sn string) (bool, error) {
    // 1. 查 Redis 缓存
    ok, _ := l.svcCtx.Redis.SIsMember(ctx, "devices:registered", sn).Result()
    if ok {
        return true, nil
    }
    // 2. 缓存未命中，查 DB
    _, ok, err := l.svcCtx.DeviceModel.GetOneBySn(ctx, sn)
    if err != nil {
        return false, err
    }
    if ok {
        // 3. 回填缓存（异步，避免登录延迟）
        go l.svcCtx.Redis.SAdd(context.Background(), "devices:registered", sn)
    }
    return ok, nil
}
```

设备注册是静态数据，缓存后登录路径不再访问 DB。

### 3.4 连接风暴保护

| 层级 | 机制 | 效果 |
|------|------|------|
| go-zero HTTP | 内置限流中间件 | 拒绝超量请求，返回 429 |
| iotws 连接数 | `MaxConns` 配置 | 拒绝超出容量的 WebSocket 连接 |
| Redis cluster | 连接池排队 | 天然背压 |
| NATS (消息路径) | JetStream MaxAckPending | 不被消息淹没 |

---

## 4. 改动清单

| 文件 | 改动 | 风险 |
|------|------|------|
| `config.go` | 新增 `LoginTransportModel`、`MessageTransportModel` | 低 |
| `servicecontext.go` | 创建两条 DeviceCli 链 | 低 |
| `iotwslogic.go` | 按 connId 选择链 | 低 |
| `iotloginlogic.go` | Redis Set 缓存设备存在性 | 中（涉及缓存一致性） |
| `iotws-api.json` | 新增配置项 | 低 |

## 5. 风险与缓解

| 风险 | 缓解 |
|------|------|
| Redis 缓存与 DB 不一致 | 设备注册接口同步写 Redis；定期从 DB 全量刷新 |
| 新设备注册后缓存未命中 | 注册接口同时 `SADD devices:registered` |
| LoginCli HTTP 不可用 | TransportModel 配置 `"http,grpc"` 自动回退 gRPC |
| 旧客户端不传 URL connId | `case false` 分支直接拒绝，返回错误帧关闭连接 |

## 6. 结论

- 登录走 HTTP 同步，并发容量 ~16K 登录/秒（单 Redis），足够支撑百万设备
- 消息走 NATS 异步，保持现有高性能路径
- 改动 5 个文件，风险可控
