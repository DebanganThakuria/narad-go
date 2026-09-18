package narad

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"time"
)

// A Message is one record handed to a consumer.
//
// Settle it with [Message.Ack] when the work is done, [Message.Nack] to
// hand it straight back, or [Message.Extend] when the work is taking
// longer than the visibility timeout. [Client.Consume] does all of that
// for you.
type Message struct {
	// Topic is where it was delivered. For a fan-out child this is the
	// child, not the topic it was produced to.
	Topic string `json:"topic"`
	// Partition is which partition it came from.
	Partition int `json:"partition"`
	// Offset is its position in that partition's log.
	Offset int64 `json:"offset"`
	// Key is the partition key it was produced with, if any.
	Key string `json:"key,omitempty"`
	// Receipt is the token that proves the lease. It is opaque, and
	// empty for a message from [Client.ReadAt], which reserves nothing.
	Receipt string `json:"receipt_handle,omitempty"`

	// Payload is the body as the server encoded it. Read it with
	// [Message.Into], [Message.Bytes] or [Message.Text].
	Payload json.RawMessage `json:"payload"`
	// Encoding is "base64" when the body was binary, empty otherwise.
	Encoding string `json:"payload_encoding,omitempty"`
	// Timestamp is when the broker committed it, in Unix seconds.
	Timestamp int64 `json:"timestamp"`

	client *Client
	// env caches the decoded envelope, so the id is cheap to put in
	// every error and log line without re-parsing the payload.
	env        *Envelope
	envChecked bool
}

// ID is the envelope's message id, or empty when the message was
// produced without an envelope.
//
// It appears in the message's own error messages and in everything the
// client logs about it, which is the point of having one: a failure
// three services away can be traced back to the message that caused it.
func (m *Message) ID() string {
	if env := m.envelope(); env != nil {
		return env.ID
	}
	return ""
}

// describe names the message for an error or a log line, including the
// envelope id when there is one.
func (m *Message) describe() string {
	where := fmt.Sprintf("%s/%d@%d", m.Topic, m.Partition, m.Offset)
	if id := m.ID(); id != "" {
		return where + " id=" + id
	}
	return where
}

// Time is when the broker committed the message, to the second.
func (m *Message) Time() time.Time {
	if m.Timestamp == 0 {
		return time.Time{}
	}
	return time.Unix(m.Timestamp, 0)
}

// Leased reports whether the message holds a lease that must be settled.
// Messages from [Client.ReadAt] do not.
func (m *Message) Leased() bool { return m.Receipt != "" }

// Into decodes the message into v.
//
// It unwraps the envelope when there is one, so v is the value that was
// produced either way and a caller does not have to know which. Read the
// envelope's own fields with [Message.Envelope].
func (m *Message) Into(v any) error {
	if env := m.envelope(); env != nil {
		return env.Into(v)
	}
	raw, err := m.Bytes()
	if err != nil {
		return err
	}
	if err := json.Unmarshal(raw, v); err != nil {
		return fmt.Errorf("narad: decode %s: %w", m.describe(), err)
	}
	return nil
}

// Text returns the payload as a string. It is empty if the payload could
// not be decoded; use [Message.Bytes] when that matters.
func (m *Message) Text() string {
	raw, err := m.Bytes()
	if err != nil {
		return ""
	}
	return string(raw)
}

// Bytes returns the message body.
//
// It is best effort, and the one case it cannot get right is worth
// knowing. The broker encodes a payload three ways: valid JSON stays
// verbatim, other UTF-8 text becomes a JSON string, and binary becomes
// base64 with Encoding set. Only the third is marked on the wire, so a
// payload arriving as a JSON string is either text the broker quoted for
// transport or a JSON string the producer really sent. This unquotes it,
// which is right for the first and loses the quotes on the second.
//
// Send a JSON object and read it with [Message.Into] and the question
// never comes up.
func (m *Message) Bytes() ([]byte, error) {
	if len(m.Payload) == 0 || string(m.Payload) == "null" {
		return nil, nil
	}
	if m.Encoding == "base64" {
		var encoded string
		if err := json.Unmarshal(m.Payload, &encoded); err != nil {
			return nil, fmt.Errorf("narad: payload marked base64 is not a string: %w", err)
		}
		raw, err := base64.StdEncoding.DecodeString(encoded)
		if err != nil {
			return nil, fmt.Errorf("narad: decode base64 payload: %w", err)
		}
		return raw, nil
	}
	if m.Payload[0] == '"' {
		var text string
		if err := json.Unmarshal(m.Payload, &text); err == nil {
			return []byte(text), nil
		}
	}
	return m.Payload, nil
}

// String describes the message without printing its body.
func (m *Message) String() string {
	return fmt.Sprintf("narad.Message{%s, %d bytes}", m.describe(), len(m.Payload))
}

// Ack settles the message. It will not be delivered again.
//
// An ack whose reply is lost is the awkward case, and this handles it.
// The retry finds the receipt already spent and the server says 410,
// which is indistinguishable from a lease that lapsed. Since the first
// attempt failed in flight, the ack almost certainly landed, so that is
// reported as success. A 410 on the first attempt is [ErrLeaseLost],
// because there it really does mean the lease was gone before the ack
// arrived.
func (m *Message) Ack(ctx context.Context) error {
	return m.settle(ctx, opAck, nil)
}

// Nack hands the message back at once, without waiting out the
// visibility timeout, so another consumer can take it.
//
// It is not a failure signal to the broker. Narad has no dead-letter
// queue, so a message nacked forever is redelivered forever. Deal with a
// poison message by acking it and recording the failure somewhere a
// person will look.
func (m *Message) Nack(ctx context.Context) error {
	return m.settle(ctx, opNack, map[string]string{"extend": "0"})
}

// Extend restarts the visibility window, so slow work keeps its message
// instead of having it redelivered underneath.
//
// Unlike Ack it gets no benefit of the doubt: if it fails, assume the
// lease is at risk and stop the work. [Client.Consume] calls this for
// you.
func (m *Message) Extend(ctx context.Context) error {
	return m.settle(ctx, opExtend, map[string]string{"extend": "true"})
}

// settle performs an ack-family request.
func (m *Message) settle(ctx context.Context, op string, extra map[string]string) error {
	if m.client == nil {
		return fmt.Errorf("narad: %s: %w: message did not come from a client", op, ErrBadRequest)
	}
	if !m.Leased() {
		return fmt.Errorf("narad: %s %s: %w", op, m.describe(), ErrNoLease)
	}

	query := url.Values{"receipt_handle": {m.Receipt}}
	for name, value := range extra {
		query.Set(name, value)
	}
	rc := call{
		method: http.MethodPost,
		path:   "/v1/topics/" + url.PathEscape(m.Topic) + "/ack",
		query:  query.Encode(),
		// A body-less POST still has to satisfy the cross-site guard.
		contentType: "application/json",
		op:          op,
		topic:       m.Topic,
		ok:          []int{http.StatusNoContent},
		once:        true,
	}

	client := m.client
	attempts := client.cfg.attempts
	if attempts < 1 {
		attempts = 1
	}
	var last error
	mayHaveLanded := false

	for attempt := range attempts {
		_, err := client.do(ctx, rc)
		if err == nil {
			return nil
		}
		// Only an ack earns this. A 410 after an attempt that may have
		// reached the owner means the receipt was spent, and the most
		// likely thing to have spent it is that attempt.
		if op == opAck && mayHaveLanded && errorIs(err, ErrLeaseLost) {
			return nil
		}
		last = err
		if attempt == attempts-1 || !Retryable(err) {
			break
		}
		if Uncertain(err) {
			mayHaveLanded = true
		}
		if ctx.Err() != nil {
			break
		}
		if !wait(ctx, client.delay(err, attempt)) {
			return ctx.Err()
		}
	}
	return last
}
