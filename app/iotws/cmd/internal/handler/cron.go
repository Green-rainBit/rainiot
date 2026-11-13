package handler

import (
	"context"
	"strings"

	"rainiot/app/iotws/cmd/internal/logic"
	logiccron "rainiot/app/iotws/cmd/internal/logic/cron"
	"rainiot/app/iotws/cmd/internal/svc"
	"rainiot/pkg/cache/consts"
	"rainiot/pkg/util"

	"github.com/hibiken/asynq"
	"github.com/robfig/cron/v3"
)

func RegisterCron(cron *cron.Cron, serverCtx *svc.ServiceContext) {
	spec := "*/5 * * * * *" // 每隔5s执行一次，cron格式（秒，分，时，天，月，周）
	// 添加一个任务
	cron.AddFunc(spec, logiccron.NewIotsyncLogic(context.Background(), serverCtx).Iotsync)
	cron.AddFunc("@every 5m", logiccron.NewIotconnsnlogic(context.Background(), serverCtx).Iotsync)
}

func Register(serverCtx *svc.ServiceContext) *asynq.ServeMux {

	mux := asynq.NewServeMux()

	//scheduler job
	mux.Handle(consts.TaskTypeWsBalance, logic.NewWsBalancedHandler(serverCtx))
	msgs := serverCtx.Cache.SubscribeTaskByInstanceId(context.Background(), serverCtx.GetInstanceId())
	util.Go(
		func() {
			for msg := range msgs {
				util.Go(
					func() {
						p := msg.Payload
						i := strings.IndexByte(p, '|')
						msgId := p[:i]
						rest := p[i+1:]
						j := strings.IndexByte(rest, '|')
						event := rest[:j]
						body := rest[j+1:] // 需要时才 Unmarshal

						err := logic.NewIotTaskLogic(context.Background(), serverCtx).IotTask(event, msgId, body)
						if err != nil {
							//serverCtx.Cache.PublishTaskByMsgId(context.Background(), msgId, ctypes.ErrorInfo{Code: "50000", Message: err.Error()})
						}
					})

			}
		},
	)
	return mux
}
