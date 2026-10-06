package narad

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// storedMessage is what the broker stores for one message of a batch.
type storedMessage struct {
	payload   []byte
	key       string
	partition *int
}

// batchBroker answers POST /produce/batch the way the broker's
// ProduceBatch handler does (handlers/messaging/produce_batch.go): it
// refuses the per-produce query parameters, decodes every message with
// its encodings, refuses an empty or oversized batch, and answers 202
// with {"accepted":N}.
type batchBroker struct {
	mu       sync.Mutex
	requests int
	stored   []storedMessage
	header   http.Header
}

func (b *batchBroker) handler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		b.mu.Lock()
		defer b.mu.Unlock()
		b.requests++
		b.header = r.Header.Clone()
		if !strings.HasSuffix(r.URL.Path, "/produce/batch") || r.Method != http.MethodPost {
			http.NotFound(w, r)
			return
		}
		if r.URL.RawQuery != "" {
			writeBrokerError(w, http.StatusBadRequest, "key is set per message in a batch produce, not as a query parameter")
			return
		}
		raw, _ := io.ReadAll(r.Body)
		if len(raw) > MaxMessageBytes {
			writeBrokerError(w, http.StatusRequestEntityTooLarge, "request body too large")
			return
		}
		var body struct {
			Messages []struct {
				Key             string          `json:"key"`
				KeyEncoding     string          `json:"key_encoding"`
				Payload         json.RawMessage `json:"payload"`
				PayloadEncoding string          `json:"payload_encoding"`
				Partition       *int            `json:"partition"`
			} `json:"messages"`
		}
		dec := json.NewDecoder(bytes.NewReader(raw))
		dec.DisallowUnknownFields()
		if err := dec.Decode(&body); err != nil {
			writeBrokerError(w, http.StatusBadRequest, "invalid json: "+err.Error())
			return
		}
		if len(body.Messages) == 0 {
			writeBrokerError(w, http.StatusBadRequest, "messages required")
			return
		}
		if len(body.Messages) > MaxBatch {
			writeBrokerError(w, http.StatusBadRequest, "too many messages: more than 100 (max 100)")
			return
		}
		var batch []storedMessage
		for i, m := range body.Messages {
			msg := storedMessage{key: m.Key, payload: m.Payload, partition: m.Partition}
			if m.KeyEncoding == "base64" {
				key, err := base64.StdEncoding.DecodeString(m.Key)
				if err != nil {
					writeBrokerError(w, http.StatusBadRequest, fmt.Sprintf("message %d: invalid key: %v", i, err))
					return
				}
				msg.key = string(key)
			}
			if m.PayloadEncoding == "base64" {
				var encoded string
				if err := json.Unmarshal(m.Payload, &encoded); err != nil {
					writeBrokerError(w, http.StatusBadRequest, fmt.Sprintf("message %d: invalid payload: a base64 payload must be a JSON string", i))
					return
				}
				payload, err := base64.StdEncoding.DecodeString(encoded)
				if err != nil {
					writeBrokerError(w, http.StatusBadRequest, fmt.Sprintf("message %d: invalid payload: %v", i, err))
					return
				}
				msg.payload = payload
			}
			if len(msg.payload) == 0 {
				writeBrokerError(w, http.StatusBadRequest, fmt.Sprintf("message %d: message required", i))
				return
			}
			batch = append(batch, msg)
		}
		b.stored = append(b.stored, batch...)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		fmt.Fprintf(w, `{"accepted":%d}`+"\n", len(batch))
	}
}

func TestProduceBatchStoresEachMessageAsProduceWould(t *testing.T) {
	t.Parallel()

	b := &batchBroker{}
	c := newTestClient(t, b.handler())

	cases := []struct {
		value     any
		opts      []ProduceOption
		payload   string
		key       string
		partition int // -1 for none
	}{
		{order{ID: "o1", Amount: 5}, []ProduceOption{WithKey("customer-42")}, `{"id":"o1","amount":5}`, "customer-42", -1},
		// JSON is stored exactly as written, inside whitespace included.
		{json.RawMessage(`{"id": "o2",  "amount": 7}`), nil, `{"id": "o2",  "amount": 7}`, "", -1},
		// Whitespace around a JSON value would be lost in transit, so it
		// goes as base64 and still arrives byte for byte.
		{json.RawMessage(" {\"id\":\"o3\"}\n"), nil, " {\"id\":\"o3\"}\n", "", -1},
		// Text and bytes are not JSON; they arrive as they were.
		{"plain text <&>", nil, "plain text <&>", "", -1},
		{[]byte{0x00, 0xff, 0x10}, []ProduceOption{WithKey("\x00\x01\x83\xff"), WithPartition(2)}, "\x00\xff\x10", "\x00\x01\x83\xff", 2},
		{[]byte(`{"already":"json"}`), []ProduceOption{WithPartition(0)}, `{"already":"json"}`, "", 0},
	}
	var batch Batch
	for i, tc := range cases {
		if err := batch.Add(tc.value, tc.opts...); err != nil {
			t.Fatalf("Add(%d): %v", i, err)
		}
	}
	if batch.Len() != len(cases) {
		t.Fatalf("Len = %d, want %d", batch.Len(), len(cases))
	}

	if err := c.ProduceBatch(context.Background(), "orders", &batch); err != nil {
		t.Fatalf("ProduceBatch: %v", err)
	}

	b.mu.Lock()
	defer b.mu.Unlock()
	if b.requests != 1 {
		t.Errorf("requests = %d, want one for the whole batch", b.requests)
	}
	if got := b.header.Get("Content-Type"); got != "application/json" {
		t.Errorf("Content-Type = %q", got)
	}
	if b.header.Get("X-Narad-Client") == "" {
		t.Error("the batch did not carry X-Narad-Client")
	}
	if len(b.stored) != len(cases) {
		t.Fatalf("stored %d messages, want %d", len(b.stored), len(cases))
	}
	for i, tc := range cases {
		got := b.stored[i]
		if string(got.payload) != tc.payload {
			t.Errorf("message %d stored %q, want %q", i, got.payload, tc.payload)
		}
		if got.key != tc.key {
			t.Errorf("message %d key %q, want %q", i, got.key, tc.key)
		}
		switch {
		case tc.partition < 0 && got.partition != nil:
			t.Errorf("message %d pinned to %d, want no partition", i, *got.partition)
		case tc.partition >= 0 && (got.partition == nil || *got.partition != tc.partition):
			t.Errorf("message %d partition %v, want %d", i, got.partition, tc.partition)
		}
	}
}

func TestBatchEnvelopeIDsAreFixedWhenAdded(t *testing.T) {
	t.Parallel()

	b := &batchBroker{}
	c := newTestClient(t, b.handler())

	var batch Batch
	for i := range 3 {
		if err := batch.Add(order{ID: fmt.Sprint(i)}, WithEnvelope()); err != nil {
			t.Fatalf("Add: %v", err)
		}
	}
	// Sending the same batch twice, as a caller resolving an uncertain
	// failure would, must send the same ids so consumers can tell.
	for range 2 {
		if err := c.ProduceBatch(context.Background(), "orders", &batch); err != nil {
			t.Fatalf("ProduceBatch: %v", err)
		}
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	ids := map[string]int{}
	for _, m := range b.stored {
		env, ok := unwrap(m.payload)
		if !ok {
			t.Fatalf("stored %s, want an envelope", m.payload)
		}
		ids[env.ID]++
	}
	if len(ids) != 3 {
		t.Fatalf("ids = %v, want 3 distinct ids each sent twice", ids)
	}
	for id, n := range ids {
		if n != 2 {
			t.Errorf("id %s sent %d times, want 2", id, n)
		}
	}
}

func TestBatchAddRefusesWhatCannotFit(t *testing.T) {
	t.Parallel()

	t.Run("by count", func(t *testing.T) {
		t.Parallel()
		var batch Batch
		for i := range MaxBatch {
			if err := batch.Add(order{ID: fmt.Sprint(i)}); err != nil {
				t.Fatalf("Add(%d): %v", i, err)
			}
		}
		if err := batch.Add(order{ID: "one too many"}); !errors.Is(err, ErrBatchFull) {
			t.Fatalf("Add past MaxBatch = %v, want ErrBatchFull", err)
		}
		if batch.Len() != MaxBatch {
			t.Errorf("Len = %d, a refused message was added", batch.Len())
		}
	})

	t.Run("by size", func(t *testing.T) {
		t.Parallel()
		var batch Batch
		chunk := json.RawMessage(`"` + strings.Repeat("x", 300<<10) + `"`)
		var err error
		for err == nil {
			err = batch.Add(chunk)
		}
		if !errors.Is(err, ErrBatchFull) {
			t.Fatalf("Add = %v, want ErrBatchFull", err)
		}
		if batch.Len() != 3 {
			t.Errorf("Len = %d, want the 3 that fit in 1 MiB", batch.Len())
		}
		body := batch.body()
		if len(body) != batch.size || len(body) > MaxMessageBytes {
			t.Errorf("body is %d bytes, tracked %d, limit %d", len(body), batch.size, MaxMessageBytes)
		}
		if !json.Valid(body) {
			t.Error("the batch body is not valid JSON")
		}
	})

	t.Run("a message too large for any batch", func(t *testing.T) {
		t.Parallel()
		var batch Batch
		// Under the single-message limit, but base64 makes it larger
		// than any request the broker reads.
		if err := batch.Add(bytes.Repeat([]byte{0xff}, 900<<10)); !errors.Is(err, ErrTooLarge) {
			t.Fatalf("Add = %v, want ErrTooLarge", err)
		}
		if batch.Len() != 0 {
			t.Error("a refused message was added")
		}
	})

	t.Run("what Produce refuses", func(t *testing.T) {
		t.Parallel()
		var batch Batch
		for _, value := range []any{nil, "", []byte{}} {
			if err := batch.Add(value); !errors.Is(err, ErrBadRequest) {
				t.Errorf("Add(%#v) = %v, want ErrBadRequest", value, err)
			}
		}
		if err := batch.Add([]byte("raw"), WithEnvelope()); !errors.Is(err, ErrBadRequest) {
			t.Errorf("an envelope around raw bytes = %v, want ErrBadRequest", err)
		}
	})
}

func TestProduceBatchRefusesAnEmptyBatchWithoutSending(t *testing.T) {
	t.Parallel()

	var calls atomic.Int32
	c := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusAccepted)
	})
	ctx := context.Background()
	if err := c.ProduceBatch(ctx, "orders", nil); !errors.Is(err, ErrBadRequest) {
		t.Errorf("nil batch = %v, want ErrBadRequest", err)
	}
	if err := c.ProduceBatch(ctx, "orders", &Batch{}); !errors.Is(err, ErrBadRequest) {
		t.Errorf("empty batch = %v, want ErrBadRequest", err)
	}
	var one Batch
	_ = one.Add(order{ID: "o1"})
	if err := c.ProduceBatch(ctx, "", &one); !errors.Is(err, ErrBadRequest) {
		t.Errorf("no topic = %v, want ErrBadRequest", err)
	}
	if calls.Load() != 0 {
		t.Errorf("%d requests sent for batches that could never succeed", calls.Load())
	}
}

// All or none: the broker names the first message it refused, and the
// client must neither retry a batch that will fail the same way nor lose
// the explanation.
func TestProduceBatchReportsTheRefusedMessage(t *testing.T) {
	t.Parallel()

	var calls atomic.Int32
	c := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		writeBrokerError(w, http.StatusBadRequest, "message 1: schema: /amount: expected integer")
	})
	var batch Batch
	_ = batch.Add(order{ID: "o1"})
	_ = batch.Add(map[string]string{"amount": "lots"})

	err := c.ProduceBatch(context.Background(), "orders", &batch)
	if !errors.Is(err, ErrBadRequest) {
		t.Fatalf("err = %v, want ErrBadRequest", err)
	}
	var apiErr *Error
	if !errors.As(err, &apiErr) || !strings.HasPrefix(apiErr.Message, "message 1: ") {
		t.Errorf("error = %+v, want the broker's message naming message 1", apiErr)
	}
	if apiErr != nil && apiErr.Op != opProduceBatch {
		t.Errorf("op = %q, want %q", apiErr.Op, opProduceBatch)
	}
	if calls.Load() != 1 {
		t.Errorf("attempts = %d, want 1", calls.Load())
	}
}

func TestProduceBatchFollowsTheProduceRetryRules(t *testing.T) {
	t.Parallel()

	newBatch := func() *Batch {
		var batch Batch
		_ = batch.Add(order{ID: "o1"})
		_ = batch.Add(order{ID: "o2"})
		return &batch
	}

	t.Run("a nothing-stored 503 moves to another node", func(t *testing.T) {
		t.Parallel()
		draining := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Retry-After", "1")
			writeBrokerError(w, http.StatusServiceUnavailable, nothingStored503s[0].message)
		}))
		defer draining.Close()
		b := &batchBroker{}
		healthy := httptest.NewServer(b.handler())
		defer healthy.Close()

		c, err := New(draining.URL+","+healthy.URL, WithRetries(2),
			WithBackoff(time.Millisecond, time.Millisecond), WithoutBreaker())
		if err != nil {
			t.Fatalf("New: %v", err)
		}
		defer c.Close()
		for range 3 {
			start := time.Now()
			if err := c.ProduceBatch(context.Background(), "orders", newBatch()); err != nil {
				t.Fatalf("ProduceBatch: %v", err)
			}
			if took := time.Since(start); took > 500*time.Millisecond {
				t.Fatalf("batch took %s, it waited out the other node's Retry-After", took)
			}
		}
	})

	t.Run("cautious retries stop at a 503", func(t *testing.T) {
		t.Parallel()
		var calls atomic.Int32
		c := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
			calls.Add(1)
			writeBrokerError(w, http.StatusServiceUnavailable, nothingStored503s[1].message)
		}, WithRetries(4), WithCautiousRetries())
		err := c.ProduceBatch(context.Background(), "orders", newBatch())
		if !errors.Is(err, ErrUnavailable) || !Uncertain(err) {
			t.Fatalf("err = %v, want an uncertain ErrUnavailable", err)
		}
		if calls.Load() != 1 {
			t.Errorf("attempts = %d, want 1", calls.Load())
		}
	})

	t.Run("a lost reply is uncertain and retried by default", func(t *testing.T) {
		t.Parallel()
		var calls atomic.Int32
		b := &batchBroker{}
		accept := b.handler()
		c := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
			if calls.Add(1) == 1 {
				writeBrokerError(w, http.StatusInternalServerError, "produce failed")
				return
			}
			accept(w, r)
		})
		if err := c.ProduceBatch(context.Background(), "orders", newBatch()); err != nil {
			t.Fatalf("ProduceBatch: %v", err)
		}
		if calls.Load() != 2 {
			t.Errorf("attempts = %d, want 2", calls.Load())
		}
	})

	t.Run("a broker without batches answers not found", func(t *testing.T) {
		t.Parallel()
		// A 3.0.x router has no such route.
		c := newTestClient(t, http.NotFound)
		err := c.ProduceBatch(context.Background(), "orders", newBatch())
		if !errors.Is(err, ErrNotFound) || Retryable(err) {
			t.Fatalf("err = %v, want a non-retryable ErrNotFound", err)
		}
	})
}

// A batch is stored whole or not at all, so a count of accepted
// messages could only ever be zero or all of them. ProduceBatch reports
// the one thing that varies: whether it failed.
func TestProduceBatchReportsOnlyAnError(t *testing.T) {
	t.Parallel()

	b := &batchBroker{}
	c := newTestClient(t, b.handler())
	var produce func(context.Context, string, *Batch) error = c.ProduceBatch

	var batch Batch
	_ = batch.Add(order{ID: "o1"})
	if err := produce(context.Background(), "orders", &batch); err != nil {
		t.Fatalf("ProduceBatch: %v", err)
	}
}
