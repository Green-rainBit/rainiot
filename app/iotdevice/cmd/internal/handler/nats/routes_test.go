package natshandler

import (
	"context"
	"testing"
	"time"

	"rainiot/app/iotdevice/cmd/internal/svc"

	"github.com/ThreeDotsLabs/watermill/message"
)

// fakeQueue 用已关闭的 channel 模拟订阅，使 StartRouter 启动的消费协程立即退出。
type fakeQueue struct{}

func (f *fakeQueue) Publish(string, ...*message.Message) error { return nil }

func (f *fakeQueue) Subscribe(context.Context, string) (<-chan *message.Message, error) {
	ch := make(chan *message.Message)
	close(ch)
	return ch, nil
}

func (f *fakeQueue) Close() error { return nil }

// TestStartRouterReturns 锁定 StartRouter 非阻塞的契约。
// main() 只有在其返回后才会执行 s.Start() / server.Start()；
// 若重新引入 select{}，iotdevice 将不监听任何端口，本测试会超时失败。
func TestStartRouterReturns(t *testing.T) {
	svcCtx := &svc.ServiceContext{}

	done := make(chan struct{})
	go func() {
		StartRouter(svcCtx)
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("StartRouter 阻塞了：它必须返回，否则 main 无法启动 HTTP/gRPC 服务")
	}
}
