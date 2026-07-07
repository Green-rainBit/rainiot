package nats

import (
	"log"
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

	publisher, err := nats.NewPublisherWithNatsConn(connect, nconfig.PublisherPublishConfig, logger)
	if err != nil {
		log.Fatal(err)
	}
	sub, err := nats.NewSubscriber(nconfig.SubscriberConfig, logger)
	if err != nil {
		log.Fatal(err)
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
