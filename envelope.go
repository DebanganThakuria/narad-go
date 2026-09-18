package narad

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"
)

// envelopeVersion marks a payload as an envelope. It is a field name
// unlikely to appear in anyone's own JSON, which is what lets
// [Message.Envelope] tell a wrapped payload from a plain one instead of
// guessing.
const envelopeVersion = "_narad"

// ErrNoEnvelope means the message was produced without one.
var ErrNoEnvelope = errors.New("narad: message has no envelope")

// An Envelope carries a message alongside the things you wish you had
// when something goes wrong: an id to trace it by, when it was produced,
// how many times it has been tried, and what went wrong last time.
//
// Producing with one is opt-in, because it changes the bytes on the
// wire:
//
//	err := client.Produce(ctx, "orders", order, narad.WithEnvelope())
//
// # It does not work with a topic schema
//
// If the topic enforces a JSON Schema, that schema describes your
// message, and an envelope is a different shape. The broker validates
// before it accepts, so the produce is rejected with [ErrBadRequest].
// Pick one: a schema on the topic, or envelopes in it. (Unless the
// schema describes the envelope itself, which works but means every
// producer must use one.)
//
// # Attempts advance only when you republish
//
// Narad never rewrites a stored payload, so redelivery hands back the
// same bytes and the same Attempts. The counter moves when you call
// [Client.Retry], which acks the message and publishes a fresh copy with
// the count raised and the error recorded. That is a real republish: a
// new offset, and a new message. Redelivery through the visibility
// timeout is invisible to it.
type Envelope struct {
	// Version marks the envelope format. It is set for you.
	Version int `json:"_narad"`
	// ID identifies this message. A UUIDv7 unless you set your own, so
	// ids sort by creation time, which is what you want when reading
	// them back in a log.
	ID string `json:"id"`
	// Body is the message you produced.
	Body json.RawMessage `json:"body"`
	// ProducedAt is when the envelope was created, in Unix
	// milliseconds.
	ProducedAt int64 `json:"produced_at"`
	// Attempts is how many times this message has been published,
	// starting at 1. See the note above about what does and does not
	// move it.
	Attempts int `json:"attempts,omitempty"`
	// LastError is what went wrong on the previous attempt, empty on the
	// first.
	LastError string `json:"last_error,omitempty"`
	// Headers carries whatever else you want to travel with the message,
	// such as a trace id or a tenant. Narad itself ignores it.
	Headers map[string]string `json:"headers,omitempty"`
}

// Time is when the envelope was produced.
func (e *Envelope) Time() time.Time {
	if e.ProducedAt == 0 {
		return time.Time{}
	}
	return time.UnixMilli(e.ProducedAt)
}

// Into decodes the enveloped message into v.
func (e *Envelope) Into(v any) error {
	if len(e.Body) == 0 {
		return fmt.Errorf("narad: envelope %s has no body", e.ID)
	}
	if err := json.Unmarshal(e.Body, v); err != nil {
		return fmt.Errorf("narad: decode envelope %s: %w", e.ID, err)
	}
	return nil
}

// WithEnvelope wraps the message in an [Envelope], giving it an id, a
// timestamp and a place to record attempts.
//
// It cannot be used on a topic that enforces a JSON Schema for the
// message itself; see [Envelope].
func WithEnvelope() ProduceOption {
	return func(c *produceConfig) { c.envelope = true }
}

// WithID sets the envelope's id instead of generating one. It implies
// [WithEnvelope].
//
// Use it to carry an id your system already has, so the message can be
// traced back without a second lookup.
func WithID(id string) ProduceOption {
	return func(c *produceConfig) {
		c.envelope = true
		c.id = id
	}
}

// WithHeaders attaches metadata to the envelope, such as a trace id. It
// implies [WithEnvelope]. Calling it more than once merges.
func WithHeaders(headers map[string]string) ProduceOption {
	return func(c *produceConfig) {
		c.envelope = true
		if c.headers == nil {
			c.headers = make(map[string]string, len(headers))
		}
		for name, value := range headers {
			c.headers[name] = value
		}
	}
}

// wrap builds the envelope around an already-encoded body.
func wrap(body []byte, cfg produceConfig) ([]byte, error) {
	id := cfg.id
	if id == "" {
		generated, err := newID()
		if err != nil {
			return nil, err
		}
		id = generated
	}
	attempts := cfg.attempts
	if attempts < 1 {
		attempts = 1
	}
	raw, err := json.Marshal(Envelope{
		Version:    1,
		ID:         id,
		Body:       body,
		ProducedAt: time.Now().UnixMilli(),
		Attempts:   attempts,
		LastError:  cfg.lastError,
		Headers:    cfg.headers,
	})
	if err != nil {
		return nil, fmt.Errorf("encode envelope: %w", err)
	}
	return raw, nil
}

// Envelope returns the message's envelope.
//
// It reports [ErrNoEnvelope] for a message produced without one, which
// is the normal case for anything published by another system.
func (m *Message) Envelope() (*Envelope, error) {
	if env := m.envelope(); env != nil {
		return env, nil
	}
	return nil, fmt.Errorf("narad: %s: %w", m.describe(), ErrNoEnvelope)
}

// envelope decodes the envelope once and caches the answer, including
// the answer "there is not one".
func (m *Message) envelope() *Envelope {
	if m.envChecked {
		return m.env
	}
	m.envChecked = true
	raw, err := m.Bytes()
	if err != nil {
		return nil
	}
	if env, ok := unwrap(raw); ok {
		m.env = env
	}
	return m.env
}

// unwrap decodes an envelope, reporting whether the payload was one.
func unwrap(raw []byte) (*Envelope, bool) {
	if len(raw) == 0 || raw[0] != '{' {
		return nil, false
	}
	// Check for the marker before decoding, so a payload that merely has
	// an "id" field is not mistaken for an envelope.
	var probe map[string]json.RawMessage
	if json.Unmarshal(raw, &probe) != nil {
		return nil, false
	}
	if _, marked := probe[envelopeVersion]; !marked {
		return nil, false
	}
	var env Envelope
	if json.Unmarshal(raw, &env) != nil {
		return nil, false
	}
	return &env, true
}

// Retry acks the message and publishes it again with its attempt count
// raised and cause recorded, so the next consumer can see how many times
// this has failed and why.
//
// It only works on an enveloped message: a plain payload has nowhere to
// keep the count. It is a real republish, so the copy gets a new offset
// and a new receipt, and the original is settled. That is the difference
// between this and returning an error from a handler, which hands the
// same bytes back and leaves the count where it was.
//
// Pass a topic to send the retry somewhere else, which is how you build
// a delay or a parking queue out of a delay child. An empty topic
// republishes to the same one.
//
//	if attempts := env.Attempts; attempts >= 5 {
//		return client.Retry(ctx, msg, err, "orders-parked")
//	}
//	return client.Retry(ctx, msg, err, "")
func (c *Client) Retry(ctx context.Context, msg *Message, cause error, topic string) error {
	if msg == nil {
		return fmt.Errorf("narad: retry: %w: message is required", ErrBadRequest)
	}
	env, err := msg.Envelope()
	if err != nil {
		return fmt.Errorf("narad: retry: %w", err)
	}
	if topic == "" {
		topic = msg.Topic
	}
	reason := ""
	if cause != nil {
		reason = cause.Error()
	}

	opts := []ProduceOption{
		WithID(env.ID),
		func(pc *produceConfig) {
			pc.attempts = env.Attempts + 1
			pc.lastError = reason
		},
	}
	if len(env.Headers) > 0 {
		opts = append(opts, WithHeaders(env.Headers))
	}
	if msg.Key != "" {
		opts = append(opts, WithKey(msg.Key))
	}

	// Publish before settling. The other order risks acking the message
	// and then failing to republish it, which loses it outright; this
	// order risks a duplicate, which at-least-once already requires
	// every consumer to tolerate.
	if err := c.Produce(ctx, topic, env.Body, opts...); err != nil {
		return fmt.Errorf("narad: retry %s: republish: %w", topic, err)
	}
	if msg.Leased() {
		if err := msg.Ack(ctx); err != nil {
			return fmt.Errorf("narad: retry %s: the copy was published but the original was not acked, so it will be redelivered: %w", topic, err)
		}
	}
	return nil
}

// idClock makes ids issued in the same millisecond still sort in the
// order they were issued.
//
// A plain UUIDv7 only orders across milliseconds: two ids from the same
// one differ by random bits and sort arbitrarily. Since a busy producer
// makes many per millisecond, that would undo most of the reason for
// choosing version 7. The 12 bits the layout leaves beside the
// timestamp are used as a counter instead, which the specification
// allows for exactly this.
var idClock struct {
	sync.Mutex
	milli uint64
	seq   uint16
}

// nextTick returns the timestamp and sequence for a new id, waiting for
// the next millisecond in the very unlikely event that 4096 ids are
// issued inside one.
func nextTick() (milli uint64, seq uint16) {
	idClock.Lock()
	defer idClock.Unlock()
	for {
		now := uint64(time.Now().UnixMilli())
		switch {
		case now > idClock.milli:
			idClock.milli, idClock.seq = now, 0
			return now, 0
		case now == idClock.milli && idClock.seq < 0xfff:
			idClock.seq++
			return now, idClock.seq
		case now < idClock.milli:
			// The clock went backwards. Keep issuing against the last
			// millisecond seen rather than handing out ids that sort
			// before ones already given out.
			if idClock.seq < 0xfff {
				idClock.seq++
				return idClock.milli, idClock.seq
			}
		}
		// Out of sequence numbers for this millisecond.
		time.Sleep(time.Millisecond)
	}
}

// newID returns a UUIDv7: 48 bits of Unix milliseconds, a 12-bit
// sequence, and randomness for the rest, so ids sort by creation order.
func newID() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", fmt.Errorf("narad: generate message id: %w", err)
	}
	milli, seq := nextTick()
	// Big-endian 48-bit timestamp in the first six bytes.
	binary.BigEndian.PutUint16(b[0:2], uint16(milli>>32))
	binary.BigEndian.PutUint32(b[2:6], uint32(milli))
	// Version 7 in the high nibble of byte 6, then the sequence in the
	// 12 bits below it. Variant 10 in byte 8.
	b[6] = 0x70 | byte(seq>>8)
	b[7] = byte(seq)
	b[8] = (b[8] & 0x3f) | 0x80

	var out [36]byte
	hex.Encode(out[0:8], b[0:4])
	out[8] = '-'
	hex.Encode(out[9:13], b[4:6])
	out[13] = '-'
	hex.Encode(out[14:18], b[6:8])
	out[18] = '-'
	hex.Encode(out[19:23], b[8:10])
	out[23] = '-'
	hex.Encode(out[24:36], b[10:16])
	return string(out[:]), nil
}
