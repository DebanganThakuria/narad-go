package narad

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
)

// A ProduceOption adjusts one produce.
type ProduceOption func(*produceConfig)

type produceConfig struct {
	key       string
	partition int
	pinned    bool

	// Envelope settings. See envelope.go.
	envelope  bool
	id        string
	headers   map[string]string
	attempts  int
	lastError string
}

// WithKey sets the partition key.
//
// Keyed messages land on one partition in steady state, which keeps a
// key's messages together and makes fan-out cheap. It is not an ordering
// guarantee: Narad does not offer one, and while a node is down a key's
// messages walk forward to a live partition.
func WithKey(key string) ProduceOption {
	return func(c *produceConfig) { c.key = key }
}

// WithPartition pins the message to one partition, overriding where its
// key would have put it. The call fails if the partition does not exist.
func WithPartition(partition int) ProduceOption {
	return func(c *produceConfig) {
		c.partition, c.pinned = partition, true
	}
}

// Produce sends a message and returns once the broker has it on disk.
//
// The value decides the encoding: []byte is sent as it is, a string is
// sent as text, and anything else is marshalled to JSON. Prefer a struct
// or a map, because the broker stores valid JSON verbatim and hands it
// back verbatim, which a bare string does not do.
//
//	err := client.Produce(ctx, "orders", order)
//	err := client.Produce(ctx, "orders", order, narad.WithKey(order.Customer))
//
// Returning nil means the payload was fsynced before the broker
// answered. It does not mean anyone has consumed it.
//
// On an error, ask [Uncertain]. A produce whose reply was lost may still
// be committed, and the default retry policy will already have tried
// again, which can duplicate. Consumers have to be idempotent anyway.
func (c *Client) Produce(ctx context.Context, topic string, value any, opts ...ProduceOption) error {
	if topic == "" {
		return fmt.Errorf("narad: produce: %w: topic is required", ErrBadRequest)
	}
	var cfg produceConfig
	for _, opt := range opts {
		opt(&cfg)
	}

	payload, contentType, err := encode(value)
	if err != nil {
		return fmt.Errorf("narad: produce %s: %w", topic, err)
	}
	if len(payload) == 0 {
		return fmt.Errorf("narad: produce %s: %w: message is empty", topic, ErrBadRequest)
	}
	if cfg.envelope {
		if contentType != "application/json" {
			// The envelope is JSON, so the message inside it has to be
			// too. Wrapping raw bytes would produce a body that is not
			// valid JSON and the broker would reject it.
			return fmt.Errorf("narad: produce %s: %w: an envelope needs a JSON message, not raw bytes",
				topic, ErrBadRequest)
		}
		payload, err = wrap(payload, cfg)
		if err != nil {
			return fmt.Errorf("narad: produce %s: %w", topic, err)
		}
	}
	if len(payload) > MaxMessageBytes {
		return fmt.Errorf("narad: produce %s: %w: %d bytes, limit is %d",
			topic, ErrTooLarge, len(payload), MaxMessageBytes)
	}

	query := url.Values{}
	if cfg.key != "" {
		query.Set("key", cfg.key)
	}
	if cfg.pinned {
		query.Set("partition", strconv.Itoa(cfg.partition))
	}

	_, err = c.do(ctx, call{
		method:      http.MethodPost,
		path:        "/v1/topics/" + url.PathEscape(topic) + "/produce",
		query:       query.Encode(),
		body:        payload,
		contentType: contentType,
		op:          opProduce,
		topic:       topic,
		ok:          []int{http.StatusAccepted},
	})
	return err
}

// encode turns a value into a body and its content type.
func encode(value any) ([]byte, string, error) {
	switch v := value.(type) {
	case nil:
		return nil, "", fmt.Errorf("%w: message is nil", ErrBadRequest)
	case []byte:
		return v, "application/octet-stream", nil
	case string:
		return []byte(v), "application/octet-stream", nil
	case json.RawMessage:
		return v, "application/json", nil
	default:
		raw, err := json.Marshal(value)
		if err != nil {
			return nil, "", fmt.Errorf("encode message: %w", err)
		}
		return raw, "application/json", nil
	}
}
