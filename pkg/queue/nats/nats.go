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

	jetStreamCfg := nats.JetStreamConfig{
		Disabled:      false,
		AutoProvision: nconfig.AutoProvision,
		DurablePrefix: durablePrefix,
	}

	publisher, err := nats.NewPublisherWithNatsConn(connect, nats.PublisherPublishConfig{
		Marshaler:         &nats.NATSMarshaler{},
		SubjectCalculator: nats.DefaultSubjectCalculator,
		JetStream:         jetStreamCfg,
	}, logger)
	if err != nil {
		return nil, err
	}

	sub, err := nats.NewSubscriberWithNatsConn(connect, nats.SubscriberSubscriptionConfig{
		SubjectCalculator: nats.DefaultSubjectCalculator,
		QueueGroupPrefix:  nconfig.QueueGroupPrefix,
		SubscribersCount:  nconfig.SubscribersCount,
		JetStream:         jetStreamCfg,
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
