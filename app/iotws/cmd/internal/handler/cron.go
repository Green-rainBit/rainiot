package handler

import (
	"context"
	"rainiot/app/iotws/cmd/internal/logic"
	"rainiot/app/iotws/cmd/internal/svc"
	"rainiot/pkg/cache"

	"github.com/hibiken/asynq"
	"github.com/robfig/cron/v3"
)

func RegisterCron(cron *cron.Cron, serverCtx *svc.ServiceContext) {
	spec := "*/5 * * * * *" // 每隔5s执行一次，cron格式（秒，分，时，天，月，周）
	// 添加一个任务
	cron.AddFunc(spec, logic.NewIotsyncLogic(context.Background(), serverCtx).Iotsync)
	cron.AddFunc(spec, logic.NewIotsyncLogic(context.Background(), serverCtx).Iotsync)
}

func Register(serverCtx *svc.ServiceContext) *asynq.ServeMux {

	mux := asynq.NewServeMux()

	//scheduler job
	mux.Handle(cache.GetWsBalancedPublishCache(serverCtx.Config.Name), logic.NewWsBalancedHandler(serverCtx))

	return mux
}
