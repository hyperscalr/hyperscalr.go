package hyperscalr

import (
	"context"

	"github.com/segmentio/ksuid"
)

type Client interface {
	// PushQueueMessage pushes the message to the queue and returns the updated
	// message or an error.
	//
	// The returned message may be different than the one provided, such that
	// the PushQueueMessage implementation may wish to override properties such
	// as UniqueId to satisfy the implementation.
	PushQueueMessage(ctx context.Context, msg QueueMessage, options ...PushQueueMessageOption) (QueueMessage, error)
}

// QueueMessage represents a hyperscalr queue message. All fields must be
// populated for a push of a QueueMessage to be successful.
type QueueMessage struct {
	// UniqueId is a unique id to identify this message. It may be used for
	// de-duplication by the stream processing.
	UniqueId ksuid.KSUID

	// Pipelines are the named pipelines to place the message on. You can think
	// of a pipeline as a series of unordered named queues.
	//
	// If there are multiple Pipelines set, then the message will be pushed
	// to each one. Pushing to Pipelines is atomic from the client, but not
	// guaranteed by the hyperscalr ingress plane. Hyperscalr ingress place will
	// attempt to push to the configured Pipelines optimistically on it's
	// end. This is the same as if multiple calls by the client were made to
	// single Pipelines.
	Pipelines []Pipelines

	// Payload message body of what you want pushed into the queue.
	//
	// It does have restrictions on size. The server is likely to reject
	// QueueMessage with total size greater than 32 KB.
	Payload []byte
}

type Pipelines struct {
	Name string
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
