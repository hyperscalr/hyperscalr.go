package hyperscalr

import (
	"context"
	"fmt"

	"github.com/hyperscalr/hyperscalr.go/protocol"
	"github.com/nats-io/nats.go"
	"github.com/pkg/errors"
	"github.com/segmentio/ksuid"
)

// NatsClientOption is a function on the options for a client.
type NatsClientOption func(*NatsClientOptions) error

// Options can be used to create a customized client.
type NatsClientOptions struct {
	NatsOptions          []nats.Option
	NatsJetstreamEnabled bool
}

func NatsClientOptionsWithNatsOptions(options ...nats.Option) NatsClientOption {
	return func(o *NatsClientOptions) error {
		o.NatsOptions = append(o.NatsOptions, options...)
		return nil
	}
}

func GetDefaultNatsClientOptions() NatsClientOptions {
	return NatsClientOptions{
		NatsOptions:          getDefaultNatsOptions(),
		NatsJetstreamEnabled: false,
	}
}

func getDefaultNatsOptions() []nats.Option {
	var options []nats.Option
	options = append(options, []nats.Option{
		nats.Name(fmt.Sprintf("hyperscalr-go-%s", ksuid.New().String())),
	}...)

	return options
}

func NewNatsClient(url string, options ...NatsClientOption) (*NatsClient, error) {
	opts := GetDefaultNatsClientOptions()
	for _, opt := range options {
		if opt != nil {
			if err := opt(&opts); err != nil {
				return nil, err
			}
		}
	}

	var natsOptions []nats.Option
	natsOptions = append(natsOptions, getDefaultNatsOptions()...)
	natsOptions = append(natsOptions, opts.NatsOptions...)
	nc, err := nats.Connect(url, natsOptions...)
	if err != nil {
		return nil, errors.Wrap(err, "nats connect")
	}

	cl := NatsClient{
		opts: opts,
		nc:   nc,
	}

	if opts.NatsJetstreamEnabled {
		js, err := nc.JetStream(nats.PublishAsyncMaxPending(256))
		if err != nil {
			return nil, errors.Wrap(err, "initializing nats jetstream context")
		}
		cl.js = js
	}

	return &cl, err
}

type NatsClient struct {
	opts NatsClientOptions
	nc   *nats.Conn
	js   nats.JetStreamContext
}

func (c *NatsClient) Close() error {
	c.nc.Close()
	return nil
}

// Push will publish the message on the stream.
//
// If UniqueId on the message is not set then a random new one will be created.
//
// If there are multiple pipelines set, then the message will be pushed to all
// the pipelines. Pushing to pipelines is atomic on from the client, but not
// guaranteed by the hyperscalr plane. Hyperscalr will attempt to push to the
// configured pipelines optimistically. This is the same as if multiple calls by
// the client were made to multiple pipelines.
func (c *NatsClient) PushQueueMessage(
	ctx context.Context,
	msg QueueMessage,
	options ...PushQueueMessageOption,
) (QueueMessage, error) {
	opts := GetDefaultPushQueueMessageOptions()
	for _, opt := range options {
		if opt != nil {
			if err := opt(&opts); err != nil {
				return msg, err
			}
		}
	}

	if opts.NatsSubject == "" {
		// Must specify a nats subject
		return msg, errors.New("NatsSubject must be provided")
	}

	// If UniqueId is not set then set it.
	if ksuid.Compare(msg.UniqueId, ksuid.Nil) == 0 {
		msg.UniqueId = ksuid.New()
	}

	// TODO: Include tenant information?
	pm := protocol.QueueMessage{
		UniqueId: msg.UniqueId,
		Payload:  msg.Payload,
	}

	// If there are pipelines then set them
	if len(msg.Pipelines) > 0 {
		pm.Pipelines = make([]protocol.Pipeline, 0, len(msg.Pipelines))
		for _, p := range msg.Pipelines {
			pm.Pipelines = append(pm.Pipelines, protocol.Pipeline{
				Name: p.Name,
			})
		}
	}

	// We synchronously publish, but we don't do anything with the returned
	// ack object.
	_, err := c.js.Publish(opts.NatsSubject, pm.Bytes())
	if err != nil {
		// TODO: Retry?
		return msg, fmt.Errorf("publishing ingress push message: %w", err)
	}

	return msg, nil
}

var (
	_ Client = (*NatsClient)(nil)
)
