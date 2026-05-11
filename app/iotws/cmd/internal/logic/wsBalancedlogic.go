package logic

import (
	"context"
	"encoding/json"

	"rainiot/app/iotws/cmd/internal/svc"
	"rainiot/app/iotws/cmd/internal/types"
	"rainiot/pkg/cache"

	"github.com/hibiken/asynq"
	"github.com/lxzan/gws"
	"github.com/zeromicro/go-zero/core/logx"
)

// wsBalancedHandler shcedule billing to home business
type wsBalancedHandler struct {
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewWsBalancedHandler(svcCtx *svc.ServiceContext) *wsBalancedHandler {
	return &wsBalancedHandler{
		Logger: logx.WithContext(context.Background()),
		svcCtx: svcCtx,
	}
}

// every one minute exec : if return err != nil , asynq will retry
func (l *wsBalancedHandler) ProcessTask(ctx context.Context, task *asynq.Task) error {
	err := cache.BlockUntilLock(l.svcCtx.Redis, ctx, cache.CacheWsServerNameLock(l.svcCtx.Config.Name))
	if err != nil {
		return err
	}
	defer cache.Unlock(l.svcCtx.Redis, ctx, cache.CacheWsServerNameLock(l.svcCtx.Config.Name))

	pub := cache.WsBalancedPublish{}
	err = json.Unmarshal(task.Payload(), &pub)
	if err != nil {
		return err
	}
	if float64(l.svcCtx.Connection.GetNumber())-float64(pub.Meanws) < 20 {
		return nil
	}
	percentage := float64(l.svcCtx.Connection.GetNumber()) - float64(pub.Meanws)/float64(pub.Meanws)
	if percentage < 0.1 {
		return nil
	}
	var Amount int64
	for _, v := range pub.ReceiveWsserverAmount {
		Amount += v
	}
	conns, ok := l.svcCtx.Connection.GetConnByCount(Amount)
	if !ok {
		return nil
	}
	messages := make([][]byte, 0, len(pub.ReceiveWsserver))
	for i := range pub.ReceiveWsserver {
		by, _ := json.Marshal(types.Response{
			Cmd: "balanced",
			Data: types.WsBalanced{
				Targeted: pub.ReceiveWsserver[i],
			},
		})
		messages = append(messages, by)
	}

	severNumber := 0
	for i := 0; i < len(conns); i++ {
		if pub.ReceiveWsserverAmount[severNumber] > 0 {
			switch co := conns[i].(type) {
			case *gws.Conn:
				co.WriteMessage(gws.OpcodeText, messages[severNumber])
			default:
				// return message, conn.(*gws.Conn).WriteMessage(gws.OpcodeText, message)
			}
			pub.ReceiveWsserverAmount[severNumber]--
		}
		if pub.ReceiveWsserverAmount[severNumber] == 0 {
			severNumber++
		}
	}

	return nil
}
