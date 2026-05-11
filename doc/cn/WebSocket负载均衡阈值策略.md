# WebSocket 负载均衡阈值策略

## 目标

负载均衡不应只依赖固定时间间隔，也不应只看平均值。判断是否需要均衡时，需要同时考虑连接总量、节点差距、比例差距、距离上次均衡的时间，以及单轮迁移数量。

这套策略的目标是：

- 避免连接数很少时做无意义迁移。
- 避免节点差距很小时频繁均衡。
- 避免刚均衡完成又立刻触发下一轮。
- 避免单轮迁移过多导致目标节点瞬时抖动。

## 快照数据

每次判断前，先读取所有在线 ws 节点的连接数快照。

```go
type WsNodeSnapshot struct {
    Server string
    Conn   int64
}
```

也可以使用 map 表示：

```go
map[string]int64{
    "iotws-1": 1200,
    "iotws-2": 800,
    "iotws-3": 1000,
}
```

## 核心指标

根据快照计算以下指标：

```text
nodeCount = 节点数量
total     = 集群总连接数
mean      = 平均连接数
maxConn   = 最大节点连接数
minConn   = 最小节点连接数
diff      = maxConn - minConn
diffRatio = diff / maxConn
```

如果节点数小于 2，直接不需要均衡。

## 策略参数

推荐定义统一策略结构：

```go
type BalancePolicy struct {
    MinTotalConn    int64
    MinConnDiff     int64
    MinDiffRatio    float64
    OverMeanDiff    int64
    MinInterval     time.Duration
    MaxMovePerRound int64
    MoveRatio       float64
}
```

参数含义：

| 参数 | 含义 |
| --- | --- |
| `MinTotalConn` | 集群总连接数低于该值时不均衡 |
| `MinConnDiff` | 最大节点和最小节点至少相差多少连接才考虑均衡 |
| `MinDiffRatio` | 最大最小连接数差距比例至少达到多少才考虑均衡 |
| `OverMeanDiff` | 最大节点超过平均值至少多少连接才考虑均衡 |
| `MinInterval` | 两轮实际均衡之间的最小间隔 |
| `MaxMovePerRound` | 单轮最多迁移多少连接 |
| `MoveRatio` | 单轮迁移超出平均值部分的比例 |

## 推荐默认值

初始可以使用以下保守配置：

```text
MinTotalConn    = 100
MinConnDiff     = 50
MinDiffRatio    = 0.2
OverMeanDiff    = 30
MinInterval     = 30s
MaxMovePerRound = 200
MoveRatio       = 0.5
```

这些值不是固定规则，后续应根据实际连接规模和客户端重连成本调整。

## 触发条件

建议同时满足以下条件才执行均衡：

```text
1. ws 节点数量 >= 2
2. 集群总连接数 >= MinTotalConn
3. 最大节点和最小节点连接数差值 >= MinConnDiff
4. 最大最小差距比例 >= MinDiffRatio
5. 最大节点超过平均值 >= OverMeanDiff
6. 距离上次均衡 >= MinInterval
```

只要任意条件不满足，本轮只记录检查结果，不发布迁移任务。

## 判断伪代码

```go
func ShouldBalance(snapshot map[string]int64, lastBalancedAt time.Time, policy BalancePolicy) bool {
    if len(snapshot) < 2 {
        return false
    }

    if time.Since(lastBalancedAt) < policy.MinInterval {
        return false
    }

    var total int64
    var maxConn int64
    minConn := int64(^uint64(0) >> 1)

    for _, conn := range snapshot {
        total += conn
        if conn > maxConn {
            maxConn = conn
        }
        if conn < minConn {
            minConn = conn
        }
    }

    if total < policy.MinTotalConn {
        return false
    }

    mean := total / int64(len(snapshot))
    diff := maxConn - minConn

    if diff < policy.MinConnDiff {
        return false
    }

    if maxConn > 0 && float64(diff)/float64(maxConn) < policy.MinDiffRatio {
        return false
    }

    if maxConn-mean < policy.OverMeanDiff {
        return false
    }

    return true
}
```

## 迁移数量计算

触发均衡后，不建议一次性迁移到完全平均。更稳妥的方式是只迁移超出平均值的一部分。

```go
func CalcMoveAmount(currentConn int64, mean int64, policy BalancePolicy) int64 {
    over := currentConn - mean
    if over <= 0 {
        return 0
    }

    amount := int64(float64(over) * policy.MoveRatio)
    if amount <= 0 {
        return 0
    }

    if amount > policy.MaxMovePerRound {
        return policy.MaxMovePerRound
    }

    return amount
}
```

示例：

```text
当前节点连接数: 1200
平均连接数:     1000
超出数量:       200
MoveRatio:      0.5
计算迁移数量:   100
```

如果计算结果超过 `MaxMovePerRound`，则按 `MaxMovePerRound` 截断。

## 为什么需要多个阈值

### MinTotalConn

防止小规模连接触发无意义均衡。

例如总共只有 10 个连接，分布为 8 和 2，比例看起来差距很大，但迁移收益很低。

### MinConnDiff

防止绝对差距太小时均衡。

例如 1000 和 980，只有 20 个连接差距，通常不值得迁移。

### MinDiffRatio

防止大连接规模下只看绝对差值造成误判。

例如 10000 和 9950，虽然差了 50，但比例很小。

### OverMeanDiff

确保确实存在明显高于平均值的节点。

如果最大节点只是略高于平均值，不应该触发迁移。

### MinInterval

防止刚迁移完连接又立刻开始下一轮。

客户端重连、认证、订阅恢复都需要时间，均衡动作之间应保留冷却窗口。

## 与 TryBalance 的关系

阈值判断应放在统一入口 `TryBalance` 中。

推荐流程：

```text
TryBalance(reason)
        |
        +-- 获取全局均衡锁
        +-- 检查 MinInterval
        +-- 读取连接数快照
        +-- 执行 ShouldBalance
        +-- 计算迁移计划
        +-- 发布 balanced 指令
        +-- 更新 lastBalancedAt
```

`IotwsBalanced` 只负责根据快照生成迁移计划和发布任务，不负责判断是否应该触发。

## 实施建议

1. 先实现 `BalancePolicy`、`ShouldBalance` 和 `CalcMoveAmount`。
2. 在 `TryBalance` 中接入阈值判断。
3. 将策略参数先写为默认值，后续再放入配置文件。
4. 在日志中记录每次跳过均衡的原因，例如 `diff_too_small`、`interval_too_short`、`total_conn_too_low`。
5. 上线初期先使用保守参数，观察迁移次数、迁移数量、客户端重连耗时后再调优。
