package devicecli

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"rainiot/pkg/alarm"
	conf_cli "rainiot/pkg/configcli"
	"rainiot/pkg/devicecli/grpc"
	"rainiot/pkg/devicecli/httpc"
	"rainiot/pkg/devicecli/nats"
	"rainiot/pkg/util"

	"github.com/zeromicro/go-zero/core/logx"
	"github.com/zeromicro/go-zero/zrpc"
)

// DeviceCli 设备推送客户端接口。
type DeviceCli interface {
	Push(ctx context.Context, connId string, message []byte) ([]byte, error)
	Information() string
}

// Reloadable 支持热重载模式的客户端。
// 当配置中心的 TransportModel 变更时，调用 Reload 即可无重启切换。
type Reloadable interface {
	Reload(model string)
}

// PushMode 推送模式位掩码，支持多模式组合。
type PushMode uint8

const (
	ModeHTTP PushMode = 1 << iota // 1
	ModeGRPC                      // 2
	ModeNATS                      // 4
)

// ParseMode 将逗号分隔的模式字符串解析为位掩码。
// 例如: "grpc,nats,http" → ModeGRPC|ModeNATS|ModeHTTP
// 空字符串或无法识别时默认返回 ModeHTTP。
func ParseMode(model string) PushMode {
	if model == "" {
		return ModeHTTP
	}
	var mode PushMode
	for _, m := range strings.Split(model, ",") {
		switch strings.TrimSpace(m) {
		case "http":
			mode |= ModeHTTP
		case "grpc":
			mode |= ModeGRPC
		case "nats":
			mode |= ModeNATS
		}
	}
	if mode == 0 {
		return ModeHTTP
	}
	return mode
}

type deviceCli struct {
	mu    sync.RWMutex
	mode  PushMode
	http  DeviceCli
	grpc  DeviceCli
	nats  DeviceCli
	chain []DeviceCli // 优先级链: grpc → nats → http

	// 热重载所需的构造依赖
	serviceName string
	confCli     conf_cli.ConfigCli
	zrpcConf    zrpc.RpcClientConf
	natsConf    *nats.NatsConf
	alarmSender alarm.Sender
}

func NewDeviceCli(model, serviceName string, confCli conf_cli.ConfigCli, zrpcConf zrpc.RpcClientConf, natsConf *nats.NatsConf, alarmSender alarm.Sender) *deviceCli {
	d := &deviceCli{
		serviceName: serviceName,
		confCli:     confCli,
		zrpcConf:    zrpcConf,
		natsConf:    natsConf,
		alarmSender: alarmSender,
	}
	d.initLocked(ParseMode(model))
	return d
}

// initLocked 根据模式初始化客户端并构建链。调用方需持有 d.mu 写锁。
func (d *deviceCli) initLocked(mode PushMode) {
	d.mode = mode

	if mode&ModeGRPC != 0 {
		d.grpc = grpc.NewDeviceCli(d.serviceName, d.zrpcConf)
	}
	if mode&ModeHTTP != 0 {
		d.http = httpc.NewDeviceCli(d.serviceName, d.confCli.GetHealthyInstances)
	}
	if mode&ModeNATS != 0 && d.natsConf != nil {
		natsCli, err := nats.NewDeviceCli(d.serviceName, *d.natsConf)
		if err != nil {
			logx.WithContext(context.Background()).Error("[deviceCli] init nats client failed (disabled): %v", err)
		} else {
			d.nats = natsCli
		}
	}

	d.rebuildChain()
}

// Reload 热更新模式配置，可在运行时从配置中心回调中调用。
// model 格式与构造函数相同，如 "grpc,nats" 或 "http"。
// 新模式的客户端按需创建，不再需要的客户端被丢弃（NATS 连接会 drain）。
// 创建/关闭客户端（含网络 I/O）在锁外执行，不阻塞 Push。
func (d *deviceCli) Reload(model string) {
	newMode := ParseMode(model)

	// 快照当前状态（读锁，短暂）
	d.mu.RLock()
	if newMode == d.mode {
		d.mu.RUnlock()
		return
	}
	oldMode := d.mode
	oldGrpc := d.grpc
	oldHttp := d.http
	oldNats := d.nats
	d.mu.RUnlock()

	logx.WithContext(context.Background()).Info("[deviceCli] hot-reload mode: %v → %v", oldMode, newMode)

	// ── 锁外：创建新客户端（网络 I/O，不阻塞 Push）──
	var newGrpc, newHttp, newNats DeviceCli
	if newMode&ModeGRPC != 0 && oldGrpc == nil {
		newGrpc = grpc.NewDeviceCli(d.serviceName, d.zrpcConf)
	}
	if newMode&ModeHTTP != 0 && oldHttp == nil {
		newHttp = httpc.NewDeviceCli(d.serviceName, d.confCli.GetHealthyInstances)
	}
	if newMode&ModeNATS != 0 && oldNats == nil && d.natsConf != nil {
		var err error
		newNats, err = nats.NewDeviceCli(d.serviceName, *d.natsConf)
		if err != nil {
			logx.WithContext(context.Background()).Error("[deviceCli] hot-reload nats failed (disabled): %v", err)
		}
	}

	// ── 锁内：提交（仅字段赋值，极短）──
	d.mu.Lock()
	if d.mode != oldMode {
		// 并发的 Reload 已修改，丢弃本次结果
		d.mu.Unlock()
		closeIfPossible(newGrpc)
		closeIfPossible(newHttp)
		closeIfPossible(newNats)
		return
	}

	// 保留未变动的旧客户端
	if newMode&ModeGRPC != 0 && oldGrpc != nil {
		d.grpc = oldGrpc
	} else if newGrpc != nil {
		d.grpc = newGrpc
	} else {
		d.grpc = nil
	}
	if newMode&ModeHTTP != 0 && oldHttp != nil {
		d.http = oldHttp
	} else if newHttp != nil {
		d.http = newHttp
	} else {
		d.http = nil
	}
	if newMode&ModeNATS != 0 && oldNats != nil {
		d.nats = oldNats
	} else if newNats != nil {
		d.nats = newNats
	} else {
		d.nats = nil
	}

	d.mode = newMode
	d.rebuildChain()
	d.mu.Unlock()

	// ── 锁外：关闭被移除的旧客户端（网络 I/O，不阻塞 Push）──
	if oldMode&ModeGRPC != 0 && newMode&ModeGRPC == 0 {
		closeIfPossible(oldGrpc)
	}
	if oldMode&ModeHTTP != 0 && newMode&ModeHTTP == 0 {
		closeIfPossible(oldHttp)
	}
	if oldMode&ModeNATS != 0 && newMode&ModeNATS == 0 {
		closeIfPossible(oldNats)
	}
}

// closeIfPossible 安全关闭客户端连接。
func closeIfPossible(cli DeviceCli) {
	if cli == nil {
		return
	}
	if closer, ok := cli.(interface{ Close() }); ok {
		closer.Close()
	}
}

// rebuildChain 按优先级 grpc → nats → http 重建链。
func (d *deviceCli) rebuildChain() {
	d.chain = d.chain[:0]
	if d.grpc != nil {
		d.chain = append(d.chain, d.grpc)
	}
	if d.nats != nil {
		d.chain = append(d.chain, d.nats)
	}
	if d.http != nil {
		d.chain = append(d.chain, d.http)
	}
}

// Push 优先级回退推送。
func (d *deviceCli) Push(ctx context.Context, connId string, message []byte) ([]byte, error) {
	d.mu.RLock()
	chain := d.chain
	d.mu.RUnlock()

	if len(chain) == 0 {
		return nil, ErrNoClientAvailable
	}

	var lastErr error
	for _, cli := range chain {
		resp, err := cli.Push(ctx, connId, message)
		if err == nil {
			return resp, nil
		}
		lastErr = err
		logx.WithContext(ctx).Error("connId: ", connId, "[deviceCli] client ", cli.Information(), " push failed, fallback next: ", err.Error())
		util.Go(func() {
			if d.alarmSender != nil {
				d.alarmSender.Send(ctx, "connId: "+connId+" [deviceCli] all clients push failed: "+lastErr.Error()+d.Information())
			}
		})
	}
	util.Go(func() {
		if d.alarmSender != nil {
			d.alarmSender.Send(ctx, "connId: "+connId+" [deviceCli] all clients push failed: "+lastErr.Error()+d.Information())
		}
	})

	return nil, lastErr
}

func (d *deviceCli) Information() string {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return fmt.Sprintf("mode: %v, grpc: %v, http: %v, nats: %v", d.mode, d.grpc, d.http, d.nats)
}
