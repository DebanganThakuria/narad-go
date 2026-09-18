package narad

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"
)

type order struct {
	ID     string `json:"id"`
	Amount int64  `json:"amount"`
}

func TestProduceEncodesByType(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name     string
		value    any
		wantBody string
		wantType string
	}{
		{"bytes pass through", []byte("raw bytes"), "raw bytes", "application/octet-stream"},
		{"string passes through", "plain text", "plain text", "application/octet-stream"},
		{"struct becomes json", order{ID: "o1", Amount: 5}, `{"id":"o1","amount":5}`, "application/json"},
		{"map becomes json", map[string]int{"n": 1}, `{"n":1}`, "application/json"},
		{"raw json is json", json.RawMessage(`{"a":1}`), `{"a":1}`, "application/json"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var gotBody []byte
			var gotType string
			c := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
				gotType = r.Header.Get("Content-Type")
				gotBody, _ = io.ReadAll(r.Body)
				w.WriteHeader(http.StatusAccepted)
			})
			if err := c.Produce(context.Background(), "orders", tc.value); err != nil {
				t.Fatalf("Produce: %v", err)
			}
			if string(gotBody) != tc.wantBody {
				t.Errorf("body = %q, want %q", gotBody, tc.wantBody)
			}
			if gotType != tc.wantType {
				t.Errorf("content type = %q, want %q", gotType, tc.wantType)
			}
		})
	}
}

func TestProduceSendsKeyAndPartition(t *testing.T) {
	t.Parallel()

	var key, partition string
	c := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		key = r.URL.Query().Get("key")
		partition = r.URL.Query().Get("partition")
		w.WriteHeader(http.StatusAccepted)
	})

	err := c.Produce(context.Background(), "orders", order{ID: "o1"},
		WithKey("customer-42"), WithPartition(3))
	if err != nil {
		t.Fatalf("Produce: %v", err)
	}
	if key != "customer-42" || partition != "3" {
		t.Errorf("key=%q partition=%q", key, partition)
	}
}

func TestProduceRejectsBadInput(t *testing.T) {
	t.Parallel()

	c := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		t.Error("nothing should have been sent")
	})
	ctx := context.Background()
	if err := c.Produce(ctx, "", order{}); !errors.Is(err, ErrBadRequest) {
		t.Errorf("empty topic: %v", err)
	}
	if err := c.Produce(ctx, "orders", nil); !errors.Is(err, ErrBadRequest) {
		t.Errorf("nil value: %v", err)
	}
	if err := c.Produce(ctx, "orders", []byte{}); !errors.Is(err, ErrBadRequest) {
		t.Errorf("empty bytes: %v", err)
	}
	// Caught locally rather than spending a round trip to be told.
	big := make([]byte, MaxMessageBytes+1)
	if err := c.Produce(ctx, "orders", big); !errors.Is(err, ErrTooLarge) {
		t.Errorf("oversize: %v", err)
	}
}

func TestEnvelopeRoundTrip(t *testing.T) {
	t.Parallel()

	var sent []byte
	c := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		sent, _ = io.ReadAll(r.Body)
		w.WriteHeader(http.StatusAccepted)
	})

	err := c.Produce(context.Background(), "orders", order{ID: "o1", Amount: 99},
		WithEnvelope(), WithHeaders(map[string]string{"trace": "abc"}))
	if err != nil {
		t.Fatalf("Produce: %v", err)
	}

	msg := &Message{Topic: "orders", Payload: sent, client: c}
	env, err := msg.Envelope()
	if err != nil {
		t.Fatalf("Envelope: %v", err)
	}
	if env.Version != 1 {
		t.Errorf("version = %d, want 1", env.Version)
	}
	if env.ID == "" || len(env.ID) != 36 {
		t.Errorf("id = %q, want a uuid", env.ID)
	}
	if env.Attempts != 1 {
		t.Errorf("attempts = %d, want 1 on a first publish", env.Attempts)
	}
	if env.Headers["trace"] != "abc" {
		t.Errorf("headers = %v", env.Headers)
	}
	if env.Time().IsZero() {
		t.Error("produced_at was not set")
	}

	// Into unwraps, so a caller does not have to know the message was
	// enveloped. Getting this wrong gives a silently zero struct.
	var got order
	if err := msg.Into(&got); err != nil {
		t.Fatalf("Into: %v", err)
	}
	if got.ID != "o1" || got.Amount != 99 {
		t.Errorf("Into gave %+v, want the produced order", got)
	}
	if msg.ID() != env.ID {
		t.Errorf("ID() = %q, want the envelope's %q", msg.ID(), env.ID)
	}
}

// The id has to reach the places a person looks when something breaks.
func TestEnvelopeIDAppearsInErrorsAndDescriptions(t *testing.T) {
	t.Parallel()

	c := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusAccepted)
	})
	body, err := wrap([]byte(`{"id":"o1"}`), produceConfig{id: "test-id-1"})
	if err != nil {
		t.Fatalf("wrap: %v", err)
	}
	msg := &Message{Topic: "orders", Partition: 2, Offset: 9, Payload: body, client: c}

	if !strings.Contains(msg.String(), "test-id-1") {
		t.Errorf("String() = %q, want the id in it", msg.String())
	}
	// A message with no lease cannot be acked, and the error should say
	// which message.
	ackErr := msg.Ack(context.Background())
	if !errors.Is(ackErr, ErrNoLease) {
		t.Fatalf("Ack = %v, want ErrNoLease", ackErr)
	}
	if !strings.Contains(ackErr.Error(), "test-id-1") {
		t.Errorf("error = %q, want the id in it", ackErr)
	}
}

func TestWithIDOverridesTheGeneratedOne(t *testing.T) {
	t.Parallel()

	var sent []byte
	c := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		sent, _ = io.ReadAll(r.Body)
		w.WriteHeader(http.StatusAccepted)
	})
	// WithID implies an envelope, so the caller does not have to say so
	// twice.
	if err := c.Produce(context.Background(), "orders", order{ID: "o1"}, WithID("mine")); err != nil {
		t.Fatalf("Produce: %v", err)
	}
	env, ok := unwrap(sent)
	if !ok {
		t.Fatal("WithID did not produce an envelope")
	}
	if env.ID != "mine" {
		t.Errorf("id = %q, want mine", env.ID)
	}
}

// The envelope is JSON, so what goes inside it has to be too.
func TestEnvelopeRejectsRawBytes(t *testing.T) {
	t.Parallel()

	c := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		t.Error("nothing should have been sent")
	})
	err := c.Produce(context.Background(), "orders", []byte("raw"), WithEnvelope())
	if !errors.Is(err, ErrBadRequest) {
		t.Fatalf("err = %v, want ErrBadRequest", err)
	}
}

// A plain payload that happens to carry an id field is not an envelope,
// and mistaking it for one would hand the caller the wrong fields.
func TestPlainPayloadIsNotMistakenForAnEnvelope(t *testing.T) {
	t.Parallel()

	c := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {})
	msg := &Message{
		Topic:   "orders",
		Payload: json.RawMessage(`{"id":"o1","attempts":7,"body":"not an envelope"}`),
		client:  c,
	}
	if _, err := msg.Envelope(); !errors.Is(err, ErrNoEnvelope) {
		t.Fatalf("Envelope = %v, want ErrNoEnvelope", err)
	}
	if msg.ID() != "" {
		t.Errorf("ID() = %q, want empty for a message with no envelope", msg.ID())
	}
	var got order
	if err := msg.Into(&got); err != nil {
		t.Fatalf("Into: %v", err)
	}
	if got.ID != "o1" {
		t.Errorf("Into gave %+v, want the payload decoded as-is", got)
	}
}

func TestGeneratedIDsAreUniqueAndOrdered(t *testing.T) {
	t.Parallel()

	const n = 500
	seen := make(map[string]bool, n)
	previous := ""
	for range n {
		id, err := newID()
		if err != nil {
			t.Fatalf("newID: %v", err)
		}
		if len(id) != 36 || strings.Count(id, "-") != 4 {
			t.Fatalf("id = %q, want uuid layout", id)
		}
		if id[14] != '7' {
			t.Fatalf("id = %q, want version 7", id)
		}
		if seen[id] {
			t.Fatalf("duplicate id %q", id)
		}
		seen[id] = true
		// Version 7 puts the timestamp first, so ids made in order sort
		// in order. That is the reason for choosing it.
		if previous != "" && id < previous {
			t.Fatalf("id %q sorts before the previous %q", id, previous)
		}
		previous = id
	}
}

func TestGeneratedIDsAreSafeUnderConcurrency(t *testing.T) {
	t.Parallel()

	const workers, each = 8, 100
	var mu sync.Mutex
	seen := make(map[string]bool, workers*each)
	var wg sync.WaitGroup
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range each {
				id, err := newID()
				if err != nil {
					t.Errorf("newID: %v", err)
					return
				}
				mu.Lock()
				if seen[id] {
					t.Errorf("duplicate id %q", id)
				}
				seen[id] = true
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
}

// Retry is the only thing that moves the attempt count, because the
// broker never rewrites a stored payload.
func TestRetryRepublishesWithAHigherAttemptCount(t *testing.T) {
	t.Parallel()

	var published []byte
	var acked bool
	var topics []string
	c := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.HasSuffix(r.URL.Path, "/produce"):
			published, _ = io.ReadAll(r.Body)
			topics = append(topics, strings.Split(r.URL.Path, "/")[3])
			w.WriteHeader(http.StatusAccepted)
		default:
			acked = true
			w.WriteHeader(http.StatusNoContent)
		}
	})

	body, err := wrap([]byte(`{"id":"o1"}`), produceConfig{id: "keep-me", attempts: 2})
	if err != nil {
		t.Fatalf("wrap: %v", err)
	}
	msg := &Message{Topic: "orders", Receipt: "1:2:3", Key: "k", Payload: body, client: c}

	if err := c.Retry(context.Background(), msg, errors.New("downstream down"), ""); err != nil {
		t.Fatalf("Retry: %v", err)
	}
	env, ok := unwrap(published)
	if !ok {
		t.Fatal("the republished message is not an envelope")
	}
	if env.Attempts != 3 {
		t.Errorf("attempts = %d, want 3", env.Attempts)
	}
	if env.LastError != "downstream down" {
		t.Errorf("last error = %q", env.LastError)
	}
	// The id follows the message, which is the whole point of having
	// one: the retry is traceable to the original.
	if env.ID != "keep-me" {
		t.Errorf("id = %q, want it preserved across the retry", env.ID)
	}
	if !acked {
		t.Error("the original was not settled")
	}
	if len(topics) != 1 || topics[0] != "orders" {
		t.Errorf("republished to %v, want the same topic", topics)
	}
}

func TestRetryCanSendElsewhere(t *testing.T) {
	t.Parallel()

	var topic string
	c := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/produce") {
			topic = strings.Split(r.URL.Path, "/")[3]
			w.WriteHeader(http.StatusAccepted)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	})

	body, _ := wrap([]byte(`{"id":"o1"}`), produceConfig{})
	msg := &Message{Topic: "orders", Receipt: "1:2:3", Payload: body, client: c}
	if err := c.Retry(context.Background(), msg, nil, "orders-parked"); err != nil {
		t.Fatalf("Retry: %v", err)
	}
	if topic != "orders-parked" {
		t.Errorf("republished to %q, want orders-parked", topic)
	}
}

func TestRetryNeedsAnEnvelope(t *testing.T) {
	t.Parallel()

	c := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		t.Error("nothing should have been sent")
	})
	msg := &Message{Topic: "orders", Receipt: "1:2:3", Payload: json.RawMessage(`{"id":"o1"}`), client: c}
	if err := c.Retry(context.Background(), msg, nil, ""); !errors.Is(err, ErrNoEnvelope) {
		t.Fatalf("Retry = %v, want ErrNoEnvelope", err)
	}
}

// Publishing before settling risks a duplicate; settling first risks
// losing the message outright. The order is deliberate and worth
// pinning.
func TestRetryDoesNotAckWhenTheRepublishFails(t *testing.T) {
	t.Parallel()

	var acked bool
	c := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/produce") {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		acked = true
		w.WriteHeader(http.StatusNoContent)
	})

	body, _ := wrap([]byte(`{"id":"o1"}`), produceConfig{})
	msg := &Message{Topic: "orders", Receipt: "1:2:3", Payload: body, client: c}
	if err := c.Retry(context.Background(), msg, nil, ""); err == nil {
		t.Fatal("Retry should have failed")
	}
	if acked {
		t.Error("the original was acked although the republish failed, losing the message")
	}
}

func TestEnvelopeSchemaWrapsTheUserSchema(t *testing.T) {
	t.Parallel()

	user := json.RawMessage(`{
		"$schema": "https://json-schema.org/draft/2020-12/schema",
		"type": "object",
		"properties": {"id": {"$ref": "#/$defs/id"}},
		"required": ["id"],
		"$defs": {"id": {"type": "string"}}
	}`)

	wrapped, err := EnvelopeSchema(user)
	if err != nil {
		t.Fatalf("EnvelopeSchema: %v", err)
	}
	var got map[string]any
	if err := json.Unmarshal(wrapped, &got); err != nil {
		t.Fatalf("result is not JSON: %v", err)
	}

	// The definitions have to be lifted to the root, or the "#/$defs/id"
	// reference inside the body would no longer resolve.
	if _, ok := got["$defs"]; !ok {
		t.Error("$defs was not lifted to the root")
	}
	if got["$schema"] == nil {
		t.Error("the dialect was dropped")
	}
	props, _ := got["properties"].(map[string]any)
	body, ok := props["body"].(map[string]any)
	if !ok {
		t.Fatalf("no body subschema: %v", props)
	}
	if _, leaked := body["$defs"]; leaked {
		t.Error("$defs was left inside the body as well as lifted")
	}
	if _, leaked := body["$schema"]; leaked {
		t.Error("$schema was left inside the body, which would re-anchor references")
	}
	if body["required"] == nil {
		t.Error("the user's own constraints were lost")
	}
	for _, field := range []string{"_narad", "id", "produced_at", "attempts", "last_error", "headers"} {
		if _, ok := props[field]; !ok {
			t.Errorf("the envelope's %q field is not described", field)
		}
	}
}

func TestEnvelopeSchemaRejectsNonsense(t *testing.T) {
	t.Parallel()

	for _, bad := range []string{"", "   ", "[1,2]", "not json", `"a string"`} {
		if _, err := EnvelopeSchema(json.RawMessage(bad)); err == nil {
			t.Errorf("EnvelopeSchema(%q) should have failed", bad)
		}
	}
}

// A schema that fails to wrap has to surface from the call the caller
// made, not as a puzzling 400 from the broker later.
func TestWithEnvelopeSchemaReportsABadSchema(t *testing.T) {
	t.Parallel()

	c := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		t.Error("nothing should have been sent")
	})
	_, err := c.CreateTopic(context.Background(), "orders",
		WithEnvelopeSchema(json.RawMessage(`[not a schema]`)))
	if !errors.Is(err, ErrBadRequest) {
		t.Fatalf("err = %v, want ErrBadRequest", err)
	}
}

func TestEnvelopeTimeAndInto(t *testing.T) {
	t.Parallel()

	env := &Envelope{ID: "x", ProducedAt: time.Now().UnixMilli(), Body: json.RawMessage(`{"id":"o1"}`)}
	if env.Time().IsZero() {
		t.Error("Time should be set")
	}
	var got order
	if err := env.Into(&got); err != nil || got.ID != "o1" {
		t.Errorf("Into gave %+v, err %v", got, err)
	}
	if err := (&Envelope{ID: "x"}).Into(&got); err == nil {
		t.Error("an envelope with no body should not decode")
	}
	if (&Envelope{}).Time().IsZero() != true {
		t.Error("a zero timestamp should give the zero time")
	}
}
