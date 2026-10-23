package logic

import (
	"context"
	"fmt"

	"rainiot/app/iotcron/cmd/internal/svc"
	plog "rainiot/pkg/log"

	"github.com/robfig/cron/v3"
)

type CronJob struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

func NewCronJob(ctx context.Context, svcCtx *svc.ServiceContext) *CronJob {
	return &CronJob{
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

// register job
func (l *CronJob) Register(logx plog.Loggerx) *cron.Cron {
	c := cron.New(cron.WithChain(
		cron.Recover(logx),
		cron.SkipIfStillRunning(logx),
	))
	// 添加定时任务
	c.AddFunc("@every 5m", func() {
		NewIotwsBalancedLogic(l.ctx, l.svcCtx).TryBalance("even")
	})
	c.AddFunc("@hourly", func() {
		fmt.Println("每小时执行一次任务")
	})
	return c
}
