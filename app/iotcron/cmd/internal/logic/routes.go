package logic

import (
	"context"
	"fmt"
	"rainiot/app/iotcron/cmd/internal/svc"

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
func (l *CronJob) Register() *cron.Cron {
	c := cron.New()
	// 添加定时任务
	c.AddFunc("@every 1s", func() {
		fmt.Println("每秒执行一次任务")
	})
	c.AddFunc("@hourly", func() {
		fmt.Println("每小时执行一次任务")
	})
	return c
}
