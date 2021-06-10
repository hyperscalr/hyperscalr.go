package protocol

import (
	"fmt"
	"net/http"
	"net/url"
	"testing"

	"github.com/segmentio/ksuid"
	"github.com/stretchr/testify/assert"
)

func TestQueueMessageMarshalUnmarshalBinary(t *testing.T) {
	pipelines := make([]Pipeline, 5)
	for i := range pipelines {
		pipelines[i].Name = fmt.Sprintf("P%d", i)
	}

	dstReq := &Webhook{
		Method: "GET",
	}
	headers := http.Header{}
	headers.Set("PIng", "pong")
	headers.Set("foo", "bar")
	headers.Set("foo", "baz")
	if err := dstReq.SetHeaders(headers); err != nil {
		t.Fatal(err)
	}
	queryParams := url.Values{}
	queryParams.Add("beep", "boop")
	queryParams.Add("hello", "world")
	if err := dstReq.SetQueryParams(queryParams); err != nil {
		t.Fatal(err)
	}
	u, err := url.ParseRequestURI("http://example.com?arg=123")
	if err != nil {
		t.Fatal(err)
	}
	if err := dstReq.SetUrl(*u); err != nil {
		t.Fatal(err)
	}

	ism := QueueMessage{
		UniqueId:           ksuid.Max,
		Payload:            []byte(`{"foo": "bar"}`),
		Pipelines:          pipelines,
		WebhookDestination: dstReq,
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

	// Verify destination http request
	assert.Equal(t, dstReq.Method, out.WebhookDestination.Method)
	assert.Equal(t, dstReq.Headers, out.WebhookDestination.Headers)
	assert.Equal(t, dstReq.QueryParams, out.WebhookDestination.QueryParams)
	assert.Equal(t, dstReq.Url, out.WebhookDestination.Url)

	outHeaders, err := out.WebhookDestination.UnmarshalHeaders()
	if err != nil {
		t.Error(err)
	}
	assert.Equal(t, headers, outHeaders)

	outQueryParams, err := out.WebhookDestination.UnmarshalQueryParams()
	if err != nil {
		t.Error(err)
	}
	assert.Equal(t, queryParams, outQueryParams)

	outUrl, err := out.WebhookDestination.UnmarshalUrl()
	if err != nil {
		t.Error(err)
	}
	assert.Equal(t, *u, outUrl)
}
