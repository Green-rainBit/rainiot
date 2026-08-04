package nats

import (
	"strings"
	"time"

	"rainiot/pkg/openconfig"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill-nats/v2/pkg/nats"
	ns "github.com/nats-io/nats.go"
)

type natscilent struct {
	*nats.Publisher
	*nats.Subscriber
}

func NewNatsConnect(nconfig openconfig.NATSConfig, logger watermill.LoggerAdapter) (*natscilent, error) {
	connect, err := ns.Connect(strings.Join(nconfig.Addresses, ","),
		ns.MaxReconnects(-1),
		ns.ReconnectWait(3*time.Second),
	)
	if err != nil {
		logger.Error("NATS Connect Error", err, nil)
		return nil, err
	}

	// DurablePrefix 未设置时回退到 Subject，保证兼容
	durablePrefix := nconfig.DurablePrefix
	if durablePrefix == "" {
		durablePrefix = nconfig.Subject
	}

	var subOpts []ns.SubOpt
	if nconfig.MaxAckPending > 0 {
		subOpts = append(subOpts, ns.MaxAckPending(nconfig.MaxAckPending))
	}

	// 预先创建 JetStream stream（仅一次），避免每次 publish 时 ensureStream 带来的
	// StreamInfo API 往返开销。stream 存在后 JetStream publish 直接使用即可。
	if nconfig.AutoProvision {
		js, err := connect.JetStream()
		if err == nil {
			_, err = js.AddStream(&ns.StreamConfig{
				Name:     nconfig.Subject,
				Subjects: []string{nconfig.Subject, nconfig.Subject + ".>"},
			})
			if err != nil {
				logger.Error("NATS AddStream warning (stream may already exist)", err, nil)
			}
		}
	}

	// 发布端关闭 autoProvision：stream 已在上面预创建，无需每次 publish 都查 StreamInfo。
	pubJetStreamCfg := nats.JetStreamConfig{
		Disabled:         false,
		AutoProvision:    false, // stream 已预创建，关闭 per-publish ensureStream
		DurablePrefix:    durablePrefix,
		SubscribeOptions: subOpts,
	}

	// 订阅端保留 autoProvision：consumers 可能需要自动创建。
	subJetStreamCfg := nats.JetStreamConfig{
		Disabled:         false,
		AutoProvision:    nconfig.AutoProvision,
		DurablePrefix:    durablePrefix,
		SubscribeOptions: subOpts,
	}

	publisher, err := nats.NewPublisherWithNatsConn(connect, nats.PublisherPublishConfig{
		Marshaler:         &nats.NATSMarshaler{},
		SubjectCalculator: nats.DefaultSubjectCalculator,
		JetStream:         pubJetStreamCfg,
	}, logger)
	if err != nil {
		return nil, err
	}

	sub, err := nats.NewSubscriberWithNatsConn(connect, nats.SubscriberSubscriptionConfig{
		SubjectCalculator: nats.DefaultSubjectCalculator,
		QueueGroupPrefix:  nconfig.QueueGroupPrefix,
		SubscribersCount:  nconfig.SubscribersCount,
		JetStream:         subJetStreamCfg,
	}, logger)
	if err != nil {
		return nil, err
	}
	return &natscilent{
		Publisher:  publisher,
		Subscriber: sub,
	}, nil
}

func (n *natscilent) Close() error {
	if n.Publisher != nil {
		n.Publisher.Close()
	}
	if n.Subscriber != nil {
		n.Subscriber.Close()
	}
	return nil
}
