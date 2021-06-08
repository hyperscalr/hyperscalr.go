package protocol

import (
	"bytes"
	"encoding"
	"io"

	flatbuffers "github.com/google/flatbuffers/go"
	"github.com/hyperscalr/hyperscalr.go/flatbuf"
	"github.com/nats-io/nats.go"
	"github.com/segmentio/ksuid"
)

type QueueMessage struct {
	// The unique identifier generated by the client API.
	UniqueId ksuid.KSUID

	// The pipelines to process.
	Pipelines []Pipeline

	// The API caller's payload of the message.
	Payload []byte
}

func QueueMessageFromNATS(msg *nats.Msg) QueueMessage {
	m := QueueMessage{}
	// Unmarshal currently doesn't return any errors
	_ = m.UnmarshalBinary(msg.Data)
	return m
}

func (i *QueueMessage) Bytes() []byte {
	b := flatbuffers.NewBuilder(0)
	msg := i.toFlatbuf(b)
	b.Finish(msg)
	return b.FinishedBytes()
}

func (i *QueueMessage) MarshalBinary() ([]byte, error) {
	return i.Bytes(), nil
}

func (i *QueueMessage) NewReader() io.Reader {
	return bytes.NewReader(i.Bytes())
}

func (i *QueueMessage) UnmarshalBinary(data []byte) error {
	m := flatbuf.GetRootAsQueueMessage(data, 0)
	return i.fromFlatbuf(m)
}

func (i *QueueMessage) toFlatbuf(b *flatbuffers.Builder) flatbuffers.UOffsetT {
	// Add the Pipeline to the builder.
	pipelineOffsets := make([]flatbuffers.UOffsetT, len(i.Pipelines))
	for i, q := range i.Pipelines {
		pipelineOffsets[i] = q.toFlatbuf(b)
	}
	// Add the offsets for the pipelines in reverse so we maintain order.
	flatbuf.QueueMessageStartPipelinesVector(b, len(i.Pipelines))
	for i := len(pipelineOffsets) - 1; i >= 0; i-- {
		b.PrependUOffsetT(pipelineOffsets[i])
	}
	pipelines := b.EndVector(len(pipelineOffsets))

	originalPayload := b.CreateByteVector(i.Payload)
	ingressId := b.CreateByteVector(i.UniqueId.Bytes())

	flatbuf.QueueMessageStart(b)
	flatbuf.QueueMessageAddUniqueId(b, ingressId)
	flatbuf.QueueMessageAddPayload(b, originalPayload)
	flatbuf.QueueMessageAddPipelines(b, pipelines)
	return flatbuf.QueueMessageEnd(b)
}

func (i *QueueMessage) fromFlatbuf(m *flatbuf.QueueMessage) error {
	b, err := ksuid.FromBytes(m.UniqueIdBytes())
	if err != nil {
		return err
	}
	i.UniqueId = b
	i.Payload = m.PayloadBytes()
	i.Pipelines = make([]Pipeline, m.PipelinesLength())
	for idx := range i.Pipelines {
		obj := &flatbuf.Pipeline{}
		if ok := m.Pipelines(obj, idx); !ok {
			continue
		}
		i.Pipelines[idx].fromFlatbuf(obj)
	}
	return nil
}

type Pipeline struct {
	Name string
}

func (q *Pipeline) Bytes() []byte {
	b := flatbuffers.NewBuilder(0)
	msg := q.toFlatbuf(b)
	b.Finish(msg)
	return b.FinishedBytes()
}

func (q *Pipeline) MarshalBinary() ([]byte, error) {
	return q.Bytes(), nil
}

func (q *Pipeline) UnmarshalBinary(data []byte) error {
	m := flatbuf.GetRootAsPipeline(data, 0)
	q.fromFlatbuf(m)
	return nil
}

func (q *Pipeline) toFlatbuf(b *flatbuffers.Builder) flatbuffers.UOffsetT {
	pipelineName := b.CreateByteString([]byte(q.Name))

	flatbuf.PipelineStart(b)
	flatbuf.PipelineAddName(b, pipelineName)
	return flatbuf.PipelineEnd(b)
}

func (q *Pipeline) fromFlatbuf(m *flatbuf.Pipeline) {
	q.Name = string(m.Name())
}

var (
	_ encoding.BinaryMarshaler   = (*QueueMessage)(nil)
	_ encoding.BinaryUnmarshaler = (*QueueMessage)(nil)
	_ encoding.BinaryMarshaler   = (*Pipeline)(nil)
	_ encoding.BinaryUnmarshaler = (*Pipeline)(nil)
)