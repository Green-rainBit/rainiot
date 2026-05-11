# WebSocket 负载均衡触发模型设计

## 背景

当前 `IotwsBalanced` 作为定时任务执行。如果只依赖固定间隔，容易出现两个问题：

- 间隔过短：客户端刚被迁移，连接还未稳定，又触发下一轮均衡，造成重复迁移。
- 间隔过长：某个 ws 节点连接数快速升高时，必须等到下一轮定时任务才会处理。

因此建议将负载均衡拆成“触发”和“执行”两层：触发层只提醒系统可能需要检查均衡，执行层统一判断是否真的需要均衡。

## 最终模型

推荐模型：

```text
事件触发负责及时性
最小间隔负责防抖
负载阈值负责避免无意义均衡
全局锁负责并发安全
低频 cron 负责兜底
```

整体流程：

```text
iotws 连接数变化
        |
        v
写入 Redis 当前连接数
        |
        v
发布 balance-check 事件
        |
        v
iotcron 调用 TryBalance(reason)
        |
        +-- 获取全局均衡锁
        +-- 检查最小均衡间隔
        +-- 读取 ws 节点连接数快照
        +-- 判断是否超过负载阈值
        +-- 生成迁移计划
        +-- 发布 balanced 指令任务
        +-- 更新最近均衡时间
```

## 核心职责拆分

### TryBalance

`TryBalance` 是所有触发源的统一入口。

定时任务、事件触发、手动触发都不直接调用 `IotwsBalanced`，而是调用：

```go
func (l *IotwsBalancedLogic) TryBalance(reason string)
```

它负责：

- 获取全局锁，避免多个触发源同时执行均衡。
- 判断距离上次均衡是否过短。
- 加载当前 ws 节点连接数快照。
- 根据阈值判断是否需要均衡。
- 满足条件后调用实际均衡逻辑。

### IotwsBalanced

`IotwsBalanced` 保持为纯执行逻辑，只负责：

- 根据连接数快照计算平均值。
- 找出连接数过多和连接数偏少的节点。
- 生成迁移计划。
- 向连接数过多的节点发布迁移任务。

它不应关心触发来源、冷却时间、cron 间隔等调度策略。

## Redis Key 建议

### ws 节点连接数

用于保存每个 ws 节点当前连接数。

```text
iot:cache:publish:iot:ws:balanced:{serverName}
```

示例：

```text
iot:cache:publish:iot:ws:balanced:iotws-1
```

### 全局均衡锁

用于避免多个 `iotcron` 实例或多个触发源同时执行均衡。

```text
iot:ws:balance:lock
```

建议 TTL：

```text
10s ~ 30s
```

具体值应略大于一次 `TryBalance` 的正常执行耗时。

### 最近均衡时间

用于控制最小均衡间隔。

```text
iot:ws:balance:last_at
```

值可以存 Unix 秒级时间戳。

### 均衡检查事件

用于事件触发，让 `iotcron` 更快感知连接数明显变化。

```text
iot:ws:balance:check
```

事件内容可以保持轻量：

```json
{
  "server": "iotws-1",
  "conn": 1200,
  "reason": "conn_changed"
}
```

## 策略参数

建议抽象一组策略参数，后续可以放到配置文件。

```go
type BalancePolicy struct {
    MinInterval    time.Duration
    MaxCheckPeriod time.Duration
    MinConnDiff    int64
    MinDiffRatio   float64
    LockTTL        time.Duration
}
```

建议初始值：

```text
MinInterval:    30s
MaxCheckPeriod: 1m
MinConnDiff:    50
MinDiffRatio:   0.2
LockTTL:        10s
```

含义：

- `MinInterval`：两轮实际均衡之间的最小间隔，防止短时间重复迁移。
- `MaxCheckPeriod`：低频 cron 检查周期，用于兜底。
- `MinConnDiff`：最大节点和最小节点至少差多少连接才考虑均衡。
- `MinDiffRatio`：连接数差距比例至少达到多少才考虑均衡。
- `LockTTL`：全局锁过期时间。

## 均衡判断逻辑

建议先读取所有 ws 节点连接数，计算：

```text
maxConn = 最大节点连接数
minConn = 最小节点连接数
diff    = maxConn - minConn
```

满足以下条件才执行均衡：

```text
diff >= MinConnDiff
diff / maxConn >= MinDiffRatio
距离上次均衡 >= MinInterval
```

伪代码：

```go
func ShouldBalance(snapshot map[string]int64, lastBalancedAt time.Time, policy BalancePolicy) bool {
    if len(snapshot) < 2 {
        return false
    }

    if time.Since(lastBalancedAt) < policy.MinInterval {
        return false
    }

    maxConn, minConn := getMaxMin(snapshot)
    diff := maxConn - minConn
    if diff < policy.MinConnDiff {
        return false
    }

    if maxConn > 0 && float64(diff)/float64(maxConn) < policy.MinDiffRatio {
        return false
    }

    return true
}
```

## 触发方式

### 事件触发

`iotws` 在连接数明显变化时发布检查事件。

事件只表示“可能需要检查”，不表示“一定要均衡”。

`iotcron` 收到事件后调用：

```go
TryBalance("event")
```

### 低频 cron 兜底

保留 cron，但只作为兜底检查。

示例：

```go
c.AddFunc("@every 1m", func() {
    NewIotwsBalancedLogic(ctx, svcCtx).TryBalance("cron")
})
```

这样即使事件丢失，或者连接数变化没有触发事件，系统也会定期重新检查。

## 推荐落地步骤

1. 保留现有 `IotwsBalanced`，先修正为只接收快照或内部只做迁移计划生成。
2. 新增 `TryBalance(reason string)`，把锁、冷却、阈值判断集中进去。
3. 新增 Redis key：全局锁、最近均衡时间、均衡检查事件。
4. 将 cron 改为调用 `TryBalance("cron")`。
5. 在 `iotws` 连接数上报逻辑中增加事件发布，调用 `TryBalance("event")` 的消费端可以后续补充。
6. 将策略参数配置化，先使用保守默认值。

## 注意事项

- 均衡任务应尽量少迁移连接，避免一次迁移过多导致目标节点瞬时压力过大。
- `last_at` 应在真正发布迁移任务成功后更新，而不是只要进入 `TryBalance` 就更新。
- 全局锁需要设置 TTL，避免进程异常退出后锁永久残留。
- 如果 Redis `KEYS` 对生产环境有压力，应改为 `SCAN`。
- 均衡算法中需要防止节点数量为 0 或连接数快照为空导致除零。
