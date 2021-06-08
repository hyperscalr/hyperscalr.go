package protocol

import (
	"fmt"
	"testing"

	"github.com/segmentio/ksuid"
	"github.com/stretchr/testify/assert"
)

func TestQueueMessageMarshalUnmarshalBinary(t *testing.T) {
	pipelines := make([]Pipeline, 5)
	for i := range pipelines {
		pipelines[i].Name = fmt.Sprintf("P%d", i)
	}
	ism := QueueMessage{
		UniqueId:  ksuid.Max,
		Payload:   []byte(`{"foo": "bar"}`),
		Pipelines: pipelines,
	}

	// Serialize
	ipmBytes, err := ism.MarshalBinary()
	assert.NoError(t, err)

	// Deserialize
	out := &QueueMessage{}
	assert.NoError(t, out.UnmarshalBinary(ipmBytes))

	assert.Equal(t, ksuid.Max, out.UniqueId)
	assert.Equal(t, `{"foo": "bar"}`, string(out.Payload))
	assert.Equal(t, pipelines, out.Pipelines)
}
