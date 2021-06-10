package protocol

import (
	"bufio"
	"bytes"
	"context"
	"encoding"
	"io"
	"net/http"
	"net/textproto"
	urlpkg "net/url"

	flatbuffers "github.com/google/flatbuffers/go"
	"github.com/hyperscalr/hyperscalr.go/flatbuf"
	"github.com/nats-io/nats.go"
	"github.com/pkg/errors"
	"github.com/segmentio/ksuid"
)

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
	Pipelines []Pipeline

	// Payload message body of what you want pushed into the queue.
	//
	// It does have restrictions on size. The server is likely to reject
	// QueueMessage with total size greater than 32 KB.
	Payload []byte

	// The destination where the payload will be sent.
	DestinationWebhook *Webhook
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
	// Create the destination http request
	var destinationHttpRequest flatbuffers.UOffsetT
	if i.DestinationWebhook != nil {
		destinationHttpRequest = i.DestinationWebhook.toFlatbuf(b)
	}

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
	// Add the destination http request to the buffer if present.
	// TODO: Write tests for both nil and not nil
	if i.DestinationWebhook != nil {
		flatbuf.QueueMessageAddDestinationWebhook(b, destinationHttpRequest)
	}
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
	dstHttpReq := m.DestinationWebhook(nil)
	if dstHttpReq != nil {
		i.DestinationWebhook = &Webhook{}
		i.DestinationWebhook.fromFlatbuf(dstHttpReq)
	}
	return nil
}

type Pipeline struct {
	// The name of the pipeline.
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

// TODO: Rename this, it's too long.
type Webhook struct {
	// The http request Method, i.e. GET, POST.
	Method string

	// Headers to be set in the http request.
	// To understand how this is serialized, see:
	//   - https://golang.org/pkg/net/http/#Header.Write
	//   - https://golang.org/pkg/net/textproto/#Reader.ReadMIMEHeader
	Headers []byte

	// URL encoded query paramaters to be set in the http request.
	// This is marshaled and unmarshaled using Encode and ParseQuery.
	//   - https://golang.org/pkg/net/url/#Values.Encode
	//   - https://golang.org/pkg/net/url/#ParseQuery
	QueryParams string

	// URL to send the request to. You may include query parameters in the Url.
	// The QueryParams specified seperately will be appended to this Url before
	// making the request.
	//
	// This is marshaled and unmarshaled using MarshalBinary and UnmarshalBinary.
	//   - https://golang.org/pkg/net/url/#Values
	//   - https://golang.org/pkg/net/url/#ParseRequestURI
	//   - https://golang.org/pkg/net/url/#URL.MarshalBinary
	//   - https://golang.org/pkg/net/url/#URL.UnmarshalBinary
	Url []byte
}

// NewWebhook is a conviencee factory for creating a Webhook. It performs
// validation, so if possible it's better to call this function once and re-use
// the resulting object.
func NewWebhook(
	method string,
	headers map[string][]string,
	queryParams urlpkg.Values,
	url string,
) (*Webhook, error) {
	req := &Webhook{}

	// Validate method and url by creating a new request and then ignoring the
	// resulting request object. An error will be returned if this request would
	// be invalid, i.e. invalid method or url. This helps to prevent messages
	// from entering the pipeline that would otherwise end up an a dead letter
	// queue because they are invalid.
	{
		_, err := http.NewRequestWithContext(context.Background(), method, url, nil)
		if err != nil {
			return nil, errors.Wrap(err, "invalid http method")
		}
	}

	// We don't allow empty method. Go will assume empty means GET for legacy
	// documentation reasons but we want to be more strict than that.
	if method == "" {
		return nil, errors.New("method cannot be empty")
	}

	// Set method if it is present.
	req.Method = method

	// Set headers if they are present.
	if headers != nil {
		if err := req.SetHeaders(http.Header(headers)); err != nil {
			return nil, errors.Wrap(err, "new destination http request")
		}
	}

	// Set query params if they are present.
	if queryParams != nil {
		if err := req.SetQueryParams(queryParams); err != nil {
			return nil, errors.Wrap(err, "new destination http request")
		}
	}

	if url == "" {
		return nil, errors.New("url cannot be empty")
	}

	u, err := urlpkg.ParseRequestURI(url)
	if err != nil {
		return nil, errors.Wrap(err, "new destination http request: parse url")
	}
	if err := req.SetUrl(*u); err != nil {
		return nil, errors.Wrap(err, "new destination http request")
	}
	return req, nil
}

func (r *Webhook) MarshalHeaders(headers http.Header) ([]byte, error) {
	var b bytes.Buffer
	w := bufio.NewWriter(&b)
	if err := headers.Write(w); err != nil {
		return nil, errors.Wrap(err, "marshal headers")
	}
	w.Flush()
	return b.Bytes(), nil
}

func (r *Webhook) UnmarshalHeaders() (http.Header, error) {
	reader := textproto.NewReader(
		bufio.NewReader(
			io.LimitReader(
				bytes.NewReader(r.Headers),
				1<<14, // limit to 16 KiB to prevent dos attacks
			),
		),
	)

	mimeHeader, err := reader.ReadMIMEHeader()
	// For some reason, when it's done reading the header it returns io.EOF.
	if err != nil && err != io.EOF {
		return nil, errors.Wrap(err, "unmarshal headers")
	}
	return http.Header(mimeHeader), nil
}

func (r *Webhook) SetHeaders(headers http.Header) error {
	bytes, err := r.MarshalHeaders(headers)
	if err != nil {
		return errors.Wrap(err, "set headers")
	}
	r.Headers = bytes
	return nil
}

func (r *Webhook) MarshalQueryParams(v urlpkg.Values) ([]byte, error) {
	return []byte(v.Encode()), nil
}

func (r *Webhook) UnmarshalQueryParams() (urlpkg.Values, error) {
	v, err := urlpkg.ParseQuery(r.QueryParams)
	if err != nil {
		return nil, errors.Wrap(err, "unmarshal query params")
	}
	return v, nil
}

func (r *Webhook) SetQueryParams(v urlpkg.Values) error {
	bytes, err := r.MarshalQueryParams(v)
	if err != nil {
		return errors.Wrap(err, "set query params")
	}
	r.QueryParams = string(bytes)
	return nil
}

func (r *Webhook) MarshalUrl(u urlpkg.URL) ([]byte, error) {
	b, err := u.MarshalBinary()
	if err != nil {
		return nil, errors.Wrap(err, "marshal url")
	}
	return b, nil
}

func (r *Webhook) UnmarshalUrl() (urlpkg.URL, error) {
	u := &urlpkg.URL{}
	if err := u.UnmarshalBinary(r.Url); err != nil {
		return urlpkg.URL{}, errors.Wrap(err, "unmarshal url")
	}
	return *u, nil
}

func (r *Webhook) SetUrl(u urlpkg.URL) error {
	bytes, err := r.MarshalUrl(u)
	if err != nil {
		return errors.Wrap(err, "set url")
	}
	r.Url = bytes
	return nil
}

func (r *Webhook) Bytes() []byte {
	b := flatbuffers.NewBuilder(0)
	msg := r.toFlatbuf(b)
	b.Finish(msg)
	return b.FinishedBytes()
}

func (r *Webhook) MarshalBinary() ([]byte, error) {
	return r.Bytes(), nil
}

func (r *Webhook) UnmarshalBinary(data []byte) error {
	m := flatbuf.GetRootAsWebhook(data, 0)
	r.fromFlatbuf(m)
	return nil
}

func (r *Webhook) toFlatbuf(b *flatbuffers.Builder) flatbuffers.UOffsetT {
	method := b.CreateByteString([]byte(r.Method))
	headers := b.CreateByteString([]byte(r.Headers))
	queryParams := b.CreateByteString([]byte(r.QueryParams))
	url := b.CreateByteString([]byte(r.Url))

	flatbuf.WebhookStart(b)
	flatbuf.WebhookAddMethod(b, method)
	flatbuf.WebhookAddHeaders(b, headers)
	flatbuf.WebhookAddQueryParams(b, queryParams)
	flatbuf.WebhookAddUrl(b, url)
	return flatbuf.WebhookEnd(b)
}

func (r *Webhook) fromFlatbuf(m *flatbuf.Webhook) {
	r.Method = string(m.Method())
	r.Headers = m.HeadersBytes()
	r.QueryParams = string(m.QueryParams())
	r.Url = m.UrlBytes()
}

var (
	_ encoding.BinaryMarshaler   = (*QueueMessage)(nil)
	_ encoding.BinaryUnmarshaler = (*QueueMessage)(nil)
	_ encoding.BinaryMarshaler   = (*Pipeline)(nil)
	_ encoding.BinaryUnmarshaler = (*Pipeline)(nil)
)
