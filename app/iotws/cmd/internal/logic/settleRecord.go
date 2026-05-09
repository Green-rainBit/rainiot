package logic

import (
	"context"
	"encoding/json"
	"errors"
	"rainiot/app/iotws/cmd/internal/svc"
	"rainiot/app/iotws/cmd/internal/types"
	"rainiot/pkg/cache"

	"github.com/hibiken/asynq"
	"github.com/lxzan/gws"
)

// WsBalancedHandler   shcedule billing to home business
type WsBalancedHandler struct {
	svcCtx *svc.ServiceContext
}

func NewWsBalancedHandler(svcCtx *svc.ServiceContext) *WsBalancedHandler {
	return &WsBalancedHandler{
		svcCtx: svcCtx,
	}
}

// every one minute exec : if return err != nil , asynq will retry
func (l *WsBalancedHandler) ProcessTask(ctx context.Context, task *asynq.Task) error {
	pub := cache.WsBalancedPublish{}
	err := json.Unmarshal(task.Payload(), &pub)
	if err != nil {
		return err
	}
	conns, ok := l.svcCtx.Connection.GetConnByCount(50)
	if !ok {
		return errors.New("设备未连接")
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
