package narad

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"unicode/utf8"
)

// MaxBatch is the most messages one batch request may carry: a
// [Client.ProduceBatch], and the batch consume and batch ack that
// [WithBatch] turns on.
const MaxBatch = 100

// ErrBatchFull means a [Batch] cannot take another message: it already
// holds [MaxBatch], or the message would take the request past the
// broker's 1 MiB body limit. The message was not added. Send the batch
// and start another.
var ErrBatchFull = errors.New("narad: batch is full")

const (
	batchOpen  = `{"messages":[`
	batchClose = `]}`
)

// A Batch is a list of messages for [Client.ProduceBatch], stored all or
// none. The zero value is an empty batch, ready to use.
//
//	var batch narad.Batch
//	for _, order := range orders {
//		if err := batch.Add(order, narad.WithKey(order.Customer)); err != nil {
//			return err
//		}
//	}
//	err := client.ProduceBatch(ctx, "orders", &batch)
//
// A Batch is not safe for concurrent use.
type Batch struct {
	// elements are the messages, each already encoded the way the
	// broker reads one, so the request is built by joining them.
	elements [][]byte
	// size is the request body the batch makes so far.
	size int
}

// Add encodes a message and appends it to the batch.
//
// The value and options mean what they mean to [Client.Produce]:
// [WithKey] and [WithPartition] place this message, and [WithEnvelope]
// wraps it. The encoding happens here, so an envelope's id is fixed when
// the message is added, and sending the same batch again after an
// uncertain failure sends the same ids.
//
// It reports [ErrBatchFull] when the batch has no room for the message,
// [ErrTooLarge] when the message could not fit in any batch, and
// [ErrBadRequest] for a message Produce would refuse too.
func (b *Batch) Add(value any, opts ...ProduceOption) error {
	if len(b.elements) >= MaxBatch {
		return fmt.Errorf("%w: it holds %d messages already", ErrBatchFull, MaxBatch)
	}
	cfg := produceOptions(opts)
	payload, contentType, err := encodeMessage(value, cfg)
	if err != nil {
		return err
	}
	element := batchElement(payload, contentType, cfg)

	// One message alone in a batch makes the smallest request it can
	// be part of.
	if alone := len(batchOpen) + len(element) + len(batchClose); alone > MaxMessageBytes {
		return fmt.Errorf("%w: %d bytes as a batch request, limit is %d",
			ErrTooLarge, alone, MaxMessageBytes)
	}
	size := b.size + len(element)
	if len(b.elements) == 0 {
		size += len(batchOpen) + len(batchClose)
	} else {
		size++ // the comma
	}
	if size > MaxMessageBytes {
		return fmt.Errorf("%w: the message would take the request to %d bytes, limit is %d",
			ErrBatchFull, size, MaxMessageBytes)
	}
	b.elements = append(b.elements, element)
	b.size = size
	return nil
}

// Len reports how many messages the batch holds.
func (b *Batch) Len() int { return len(b.elements) }

// body joins the encoded messages into the request body.
func (b *Batch) body() []byte {
	body := make([]byte, 0, b.size)
	body = append(body, batchOpen...)
	body = append(body, bytes.Join(b.elements, []byte{','})...)
	return append(body, batchClose...)
}

// batchElement encodes one message the way the broker's batch produce
// reads it. A JSON payload goes as it is, and the broker stores exactly
// those bytes; anything else goes as base64, which is the only way to
// carry bytes that are not JSON and keeps them byte for byte. A key that
// is not valid UTF-8 goes as base64 too, as a consume returns one.
func batchElement(payload []byte, contentType string, cfg produceConfig) []byte {
	var out bytes.Buffer
	out.WriteString(`{"payload":`)
	// The broker keeps a JSON value's bytes as written, but not the
	// whitespace around it, so a payload with any goes as base64 to
	// arrive exactly as a single produce would have stored it.
	if contentType == "application/json" && json.Valid(payload) &&
		len(bytes.TrimSpace(payload)) == len(payload) {
		out.Write(payload)
	} else {
		out.WriteByte('"')
		out.WriteString(base64.StdEncoding.EncodeToString(payload))
		out.WriteString(`","payload_encoding":"base64"`)
	}
	if cfg.key != "" {
		out.WriteString(`,"key":`)
		if utf8.ValidString(cfg.key) {
			quoted, _ := json.Marshal(cfg.key) // a valid UTF-8 string always encodes
			out.Write(quoted)
		} else {
			out.WriteByte('"')
			out.WriteString(base64.StdEncoding.EncodeToString([]byte(cfg.key)))
			out.WriteString(`","key_encoding":"base64"`)
		}
	}
	if cfg.pinned {
		out.WriteString(`,"partition":`)
		out.WriteString(strconv.Itoa(cfg.partition))
	}
	out.WriteByte('}')
	return out.Bytes()
}

// ProduceBatch sends every message in the batch and returns once the
// broker has all of them on disk. It is all or nothing: a message the
// broker refuses fails the whole batch, the error's message names it
// ("message 3: ..."), and nothing is stored.
//
// Use it when messages belong together, or to cut the cost of producing
// many at once: one request and one disk sync carry up to [MaxBatch]
// messages. [Client.Produce] stays the call for one message.
//
// Failures are read as for Produce, with one thing added. A batch whose
// reply was lost may have been stored, so ask [Uncertain]; the default
// retry policy resends the whole batch, which can duplicate it. A batch
// the broker failed partway through storing can also have its leading
// messages delivered. Either way at-least-once holds, and consumers have
// to be idempotent already.
//
// A broker older than 3.1.0 has no batch endpoint and answers
// [ErrNotFound]. Fall back to Produce there, knowing it is not atomic.
// As with Produce, a batch sent to a delayed fan-out child is refused
// with [ErrBadRequest].
func (c *Client) ProduceBatch(ctx context.Context, topic string, batch *Batch) error {
	if topic == "" {
		return fmt.Errorf("narad: produce batch: %w: topic is required", ErrBadRequest)
	}
	if batch == nil || batch.Len() == 0 {
		return fmt.Errorf("narad: produce batch %s: %w: the batch is empty", topic, ErrBadRequest)
	}
	// The reply's count of accepted messages is always the whole batch,
	// so there is nothing in it to read.
	_, err := c.do(ctx, call{
		method:      http.MethodPost,
		path:        "/v1/topics/" + url.PathEscape(topic) + "/produce/batch",
		body:        batch.body(),
		contentType: "application/json",
		op:          opProduceBatch,
		topic:       topic,
		ok:          []int{http.StatusAccepted},
	})
	return err
}
