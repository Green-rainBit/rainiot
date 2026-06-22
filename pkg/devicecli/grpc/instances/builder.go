package instances

import (
	"context"
	"time"

	"google.golang.org/grpc/resolver"
)

type builder struct {
	fn func() []string
}

func NewBuilder(fn func() []string) resolver.Builder {
	return &builder{fn: fn}
}

func (b *builder) Build(target resolver.Target, conn resolver.ClientConn, opts resolver.BuildOptions) (resolver.Resolver, error) {
	ctx, cancel := context.WithCancel(context.Background())
	pipe := make(chan []string)

	go populateEndpoints(ctx, conn, pipe)

	addrs := b.fn()
	pipe <- addrs

	closeCh := make(chan struct{})

	go func() {
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				select {
				case pipe <- b.fn():
				case <-ctx.Done():
					return
				}
			case <-closeCh:
				return
			case <-ctx.Done():
				return
			}
		}
	}()

	return &resolvr{cancelFunc: cancel, closeCh: closeCh}, nil
}

func (b *builder) Scheme() string {
	return "instances"
}
