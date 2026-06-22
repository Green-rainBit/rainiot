# rainIot

rainIot 是一个基于 [go-zero](https://github.com/zeromicro/go-zero) 微服务框架的物联网（IoT）后端平台，提供设备 WebSocket 长连接管理、设备认证及横向扩展能力。

## 架构

```
                  +-----------+
                  |  iotws    |  (WebSocket 网关)
                  |  :8888    |
                  +-----+-----+
                        |
          WebSocket ←──'──→ HTTP /notice
                        |
              转发至 /device/connect
                        |
                        v
                  +-----------+
                  | iotdevice |  (设备认证服务)
                  |  :8888    |
                  +-----+-----+
                        |
              +---------+---------+
              |                   |
          PostgreSQL            Redis
        (device 表)       (连接状态 / 发布订阅)

                  +-----------+
                  | iotcron   |  (后台任务)
                  +-----+-----+
                        |
                  Redis (pub/sub + asynq)
                        |
                  WebSocket 负载均衡调度
```

## 技术栈

| 组件 | 技术 |
|---|---|
| 语言 | Go 1.25+ |
| 微服务框架 | [go-zero](https://github.com/zeromicro/go-zero) |
| 数据库 | PostgreSQL |
| 缓存 / 发布订阅 | Redis Cluster |
| WebSocket | [gws](https://github.com/lxzan/gws) |
| 服务发现 / 配置中心 | [Nacos](https://nacos.io/) |
| 分布式任务队列 | [asynq](https://github.com/hibiken/asynq) |
| 定时任务 | [robfig/cron](https://github.com/robfig/cron) |
| 可观测性 | OpenTelemetry, Prometheus, Zipkin |

## 项目结构

```
rainIot/
├── app/
│   ├── iotdevice/         # 设备认证服务（HTTP）
│   │   ├── cmd/           # 入口、配置、handler、logic
│   │   └── model/         # 数据访问层
│   ├── iotws/             # WebSocket 网关服务
│   │   └── cmd/           # 入口、配置、handler、logic、ws 连接管理
│   └── iotcron/           # 定时任务 & 负载均衡调度
│       └── cmd/           # 入口、配置、logic
├── pkg/                   # 共享包
│   ├── cache/             # Redis 缓存、分布式锁、发布订阅
│   ├── devicecli/         # 设备服务客户端（HTTP + gRPC）
│   │   ├── grpc/rpcn/     #   Nacos gRPC resolver（Subscribe 推送模式）
│   │   └── grpc/instances/#   轮询式 gRPC resolver（无 Nacos 环境）
│   ├── configcli/         # 配置客户端抽象
│   ├── openconfig/        # Nacos 配置结构体
│   ├── util/              # 工具函数
│   └── wscli/             # WebSocket 推送客户端
├── deploy/sql/            # 数据库初始化脚本
├── doc/                   # 设计文档
└── go.mod
```

## 快速开始

### 前置条件

- Go 1.25+
- PostgreSQL（需要创建 `device` 表）
- Redis Cluster
- Nacos（可选，用于服务发现与动态配置）

### 数据库初始化

```sql
-- PostgreSQL
CREATE TABLE device (
    id BIGSERIAL PRIMARY KEY,
    sn VARCHAR(255) UNIQUE NOT NULL
);
```

### 构建

```bash
# 构建各个服务
go build -o bin/iotdevice ./app/iotdevice/cmd
go build -o bin/iotws ./app/iotws/cmd
go build -o bin/iotcron ./app/iotcron/cmd
```

### 配置

每个服务在 `cmd/etc/` 目录下有两类配置文件：

**服务配置**（如 `app/iotdevice/cmd/etc/iotdevice-api.json`）：

```json
{
  "Name": "iotdevice-api",
  "Host": "0.0.0.0",
  "Port": 8888,
  "Log": { "Mode": "file", "Path": "logs/iotdevice-api" },
  "DataSource": "postgres://user:pass@localhost:5432/rainiot?sslmode=disable",
  "CacheRedis": {
    "type": "cluster",
    "Host": "127.0.0.1:6379",
    "Pass": ""
  }
}
```

**Nacos 配置**（如 `app/iotdevice/cmd/etc/iotdevice-nacos.json`）：

```json
{
  "model": "local",
  "ipAddress": [""],
  "port": 8848,
  "username": "",
  "password": "",
  "namespaceId": "",
  "dataId": "",
  "group": ""
}
```

- `model` 设为 `"nacos"` 启用 Nacos 服务发现，设为 `"local"` 则使用静态配置。
- 也可通过环境变量 `SERVICE_NAME`、`SERVICE_IP`、`SERVICE_PORT` 覆盖服务注册参数。

#### gRPC 服务发现配置（iotws）

iotws 通过 gRPC 调用 iotdevice，服务发现由 `Rpc.Model` 字段控制：

```json
{
  "Rpc": {
    "Model": "nacos"
  }
}
```

| `Rpc.Model` | 服务发现方式 | 适用场景 |
|---|---|---|
| `"nacos"` | Nacos Subscribe 推送，实时感知实例上下线 | 有 Nacos 环境 |
| `"instances"` | 轮询 `DeviceServerMap`（10s 间隔），支持配置热更新 | 无 Nacos 的普通模式 |
| 空 / 不设置 | 不注册自定义 resolver，走 go-zero 默认直连 | 单一固定实例 |

**nacos 模式**：需配合 nacos 配置文件 `model=nacos`，同时 iotdevice 需通过 `InitNacosRegisterInstanceGrpc` 将 gRPC 服务注册到 Nacos。

**instances 模式**：通过 `DeviceServerMap` 配置静态实例映射，运行时可热更新（Nacos 配置监听或文件变更），无需 Nacos 服务发现。

```json
{
  "DeviceServerMap": {
    "iotdevice_api": ["127.0.0.1:9090"]
  }
}
```

两种模式共用同一套 `resolvr` 架构，内部通过 channel 管道将地址变更推送到 gRPC 连接池，保证长连接优势的同时支持地址热更新。

### 运行

```bash
# 启动设备认证服务
./bin/iotdevice -f app/iotdevice/cmd/etc/iotdevice-api.json -nacos app/iotdevice/cmd/etc/iotdevice-nacos.json

# 启动 WebSocket 网关
./bin/iotws -f app/iotws/cmd/etc/iotws-api.json -nacos app/iotws/cmd/etc/iotws-nacos.json

# 启动定时任务服务
./bin/iotcron -f app/iotcron/cmd/etc/iotcron.json -nacos app/iotcron/cmd/etc/iotcron-nacos.json
```

## API

### 设备认证服务（iotdevice）

| 方法 | 路径 | 说明 |
|---|---|---|
| POST | `/device/connect` | 设备连接认证 |

`/device/connect` 请求体：

```json
{
  "cmd": "login",
  "sn": "DEVICE_SN_001",
  "data": {}
}
```

- `cmd` -- 命令类型，目前支持 `login`（设备登录认证）
- `sn` -- 设备序列号，必须在 `device` 表中已注册
- `data` -- 可选的附加数据

### WebSocket 网关（iotws）

| 方法 | 路径 | 说明 |
|---|---|---|
| GET | `/connect?connId=xxx` | WebSocket 连接升级 |
| POST | `/notice` | 向指定设备推送消息 |

## 负载均衡

rainIot 支持 WebSocket 多实例水平扩展，核心机制：

1. **连接上报** -- 每个 `iotws` 节点每 5 秒将自己的连接数发布到 Redis。
2. **均衡检测** -- `iotcron` 服务每 5 分钟检查所有节点的连接分布，若偏差超过 20% 则触发再平衡。
3. **再平衡** -- 通过 asynq 任务队列向过载节点下发均衡指令，过载节点通知部分客户端重连至其他节点。

详细设计见 [WebSocket 负载均衡设计文档](doc/cn/ws-balance-design.md)。

## 许可证

[Apache License 2.0](LICENSE)
