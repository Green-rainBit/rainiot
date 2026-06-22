package grpc

import (
	"context"
	"fmt"

	"github.com/nacos-group/nacos-sdk-go/v2/clients/naming_client"
	"github.com/nacos-group/nacos-sdk-go/v2/common/logger"
	"github.com/nacos-group/nacos-sdk-go/v2/model"
	"github.com/nacos-group/nacos-sdk-go/v2/vo"
	"google.golang.org/grpc/resolver"
)

type builder struct {
	client naming_client.INamingClient
}

func NewBuilder(client naming_client.INamingClient) resolver.Builder {
	return &builder{client: client}
}

func (b *builder) Build(target resolver.Target, conn resolver.ClientConn, opts resolver.BuildOptions) (resolver.Resolver, error) {
	groupName := target.URL.Query().Get("group")
	if groupName == "" {
		groupName = "DEFAULT_GROUP"
	}

	ctx, cancel := context.WithCancel(context.Background())
	pipe := make(chan []string)

	initial, err := b.client.SelectInstances(vo.SelectInstancesParam{
		ServiceName: defaultDeviceServiceName,
		GroupName:   groupName,
		HealthyOnly: true,
	})
	if err != nil {
		logger.Error("[Nacos resolver] initial SelectInstances error: %v", err)
		cancel()
		return nil, err
	}

	go populateEndpoints(ctx, conn, pipe)

	addresses := instancesToAddresses(initial)
	pipe <- addresses

	callback := func(services []model.Instance, err error) {
		if err != nil {
			logger.Error("[Nacos resolver] subscribe callback error: %v", err)
			return
		}
		select {
		case pipe <- instancesToAddresses(services):
		case <-ctx.Done():
		}
	}

	subscribeParam := &vo.SubscribeParam{
		ServiceName:       defaultDeviceServiceName,
		GroupName:         groupName,
		SubscribeCallback: callback,
	}

	if err := b.client.Subscribe(subscribeParam); err != nil {
		logger.Error("[Nacos resolver] subscribe error: %v", err)
	}

	return &resolvr{
		cancelFunc: cancel,
		unsubscribe: func() {
			if err := b.client.Unsubscribe(subscribeParam); err != nil {
				logger.Error("[Nacos resolver] unsubscribe error: %v", err)
			}
		},
	}, nil
}

func instancesToAddresses(services []model.Instance) []string {
	addrs := make([]string, 0, len(services))
	for _, s := range services {
		addr := urlForInstance(s)
		if addr != "" {
			addrs = append(addrs, addr)
		}
	}
	return addrs
}

func urlForInstance(s model.Instance) string {
	if s.Metadata != nil && s.Metadata["gRPC_port"] != "" {
		return s.Ip + ":" + s.Metadata["gRPC_port"]
	}
	return fmt.Sprintf("%s:%d", s.Ip, s.Port)
}

// Scheme returns the scheme supported by this resolver.
func (b *builder) Scheme() string {
	return "nacos"
}
