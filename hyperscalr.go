package hyperscalr

import (
	"context"

	"github.com/hyperscalr/hyperscalr.go/protocol"
)

type Client interface {
	// PushQueueMessage pushes the message to the queue and returns the updated
	// message or an error.
	//
	// The returned message may be different than the one provided, such that
	// the PushQueueMessage implementation may wish to override properties such
	// as UniqueId to satisfy the implementation.
	PushQueueMessage(ctx context.Context, msg protocol.QueueMessage, options ...PushQueueMessageOption) (protocol.QueueMessage, error)

	Drain() error
	Close() error
}

// PushQueueMessageOption is a function on the options for a PushQueueMessage
// function.
type PushQueueMessageOption func(*PushQueueMessageOptions) error

type PushQueueMessageOptions struct {
	// NatsSubject that a client might use to publish a message.
	NatsSubject string
}

func GetDefaultPushQueueMessageOptions() PushQueueMessageOptions {
	return PushQueueMessageOptions{}
}

func PushQueueMessageNatsSubject(subject string) PushQueueMessageOption {
	return func(o *PushQueueMessageOptions) error {
		o.NatsSubject = subject
		return nil
	}
}
