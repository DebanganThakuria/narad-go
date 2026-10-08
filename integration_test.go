//go:build integration

// These tests run against a real Narad cluster. The unit tests cover the
// client's logic against fakes; this covers the thing the fakes cannot,
// which is whether the wire format the client assumes is the one the
// broker actually speaks.
//
//	NARAD_ADDR=localhost:7942 NARAD_PASSWORD=... \
//	    go test -tags integration -run TestIntegration -v ./...
//
// It creates topics under a per-run prefix and deletes them afterwards,
// so it is safe to point at a shared cluster. It produces tens of
// messages, not thousands: it is a correctness check, not a load test.
package narad_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	narad "github.com/debanganthakuria/narad-go"
)

type payment struct {
	ID     string `json:"id"`
	Amount int64  `json:"amount"`
}

func dial(t *testing.T) (*narad.Client, string) {
	t.Helper()
	addr := os.Getenv("NARAD_ADDR")
	if addr == "" {
		t.Skip("set NARAD_ADDR to run the integration tests")
	}
	client, err := narad.New(addr,
		narad.WithAuth(envOr("NARAD_USER", "admin"), os.Getenv("NARAD_PASSWORD")),
		narad.WithTimeout(20*time.Second),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })
	// A prefix per run, so this is safe against a cluster somebody else
	// is also using and two runs cannot collide.
	return client, fmt.Sprintf("sdkit-%d", time.Now().UnixNano())
}

func envOr(name, fallback string) string {
	if v := os.Getenv(name); v != "" {
		return v
	}
	return fallback
}

// MinPartitions is the fewest the broker accepts. Asking for fewer is
// rejected with "partitions must be >= 3 (0 = use default)", which a
// first run of these tests discovered the hard way.
const MinPartitions = 3

// makeTopic creates a topic and removes it when the test ends.
func makeTopic(t *testing.T, client *narad.Client, name string, opts ...narad.TopicOption) narad.Topic {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	topic, err := client.EnsureTopic(ctx, name, opts...)
	if err != nil {
		t.Fatalf("EnsureTopic %s: %v", name, err)
	}
	t.Cleanup(func() {
		cleanupCtx, cancelCleanup := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancelCleanup()
		if err := client.DeleteTopic(cleanupCtx, name); err != nil {
			t.Logf("could not delete %s, please remove it by hand: %v", name, err)
		}
	})
	return topic
}

func TestIntegration(t *testing.T) {
	client, prefix := dial(t)

	t.Run("cluster is reachable", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		for _, node := range client.Nodes() {
			if err := client.Ping(ctx, node); err != nil {
				t.Errorf("Ping %s: %v", node, err)
			}
		}
		for _, health := range client.Health() {
			if !health.Healthy {
				t.Errorf("%s is unhealthy: %v", health.Address, health.LastError)
			}
		}
	})

	t.Run("topic lifecycle", func(t *testing.T) {
		name := prefix + "-topics"
		topic := makeTopic(t, client, name,
			narad.WithPartitionCount(MinPartitions),
			narad.WithRetention(2*time.Hour),
			narad.WithVisibilityTimeout(20*time.Second),
		)
		if topic.Name != name {
			t.Errorf("name = %q", topic.Name)
		}
		if topic.Partitions != 3 {
			t.Errorf("partitions = %d, want 3", topic.Partitions)
		}
		if topic.VisibilityTimeout != 20*time.Second {
			t.Errorf("visibility = %s, want 20s", topic.VisibilityTimeout)
		}

		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()

		// EnsureTopic again: the conflict must read as success.
		if _, err := client.EnsureTopic(ctx, name, narad.WithPartitionCount(9)); err != nil {
			t.Errorf("EnsureTopic on an existing topic: %v", err)
		}
		// CreateTopic must still report the conflict plainly.
		if _, err := client.CreateTopic(ctx, name); !errors.Is(err, narad.ErrExists) {
			t.Errorf("CreateTopic on an existing topic = %v, want ErrExists", err)
		}

		described, err := client.Topic(ctx, name)
		if err != nil {
			t.Fatalf("Topic: %v", err)
		}
		if len(described.PartitionStats) != 3 {
			t.Errorf("partition stats = %d, want 3", len(described.PartitionStats))
		}
		for _, stats := range described.PartitionStats {
			if stats.Owner == "" {
				t.Errorf("partition %d has no owner node", stats.Index)
			}
			if !stats.Available() {
				t.Errorf("partition %d is unavailable (%s) on a healthy cluster", stats.Index, stats.OwnerLiveness)
			}
		}
		if described.Partial {
			t.Error("a healthy cluster described the topic as partial")
		}
	})

	t.Run("topic names and retention", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		forever := makeTopic(t, client, prefix+"-forever",
			narad.WithPartitionCount(MinPartitions), narad.WithRetentionForever())
		if forever.Retention != 0 {
			t.Errorf("retention = %s, want 0, which is kept forever", forever.Retention)
		}

		// A name that differs only in letter case would share a
		// directory on a case-insensitive filesystem.
		lower := prefix + "-case"
		makeTopic(t, client, lower, narad.WithPartitionCount(MinPartitions))
		upper := strings.ToUpper(lower)
		if _, err := client.CreateTopic(ctx, upper, narad.WithPartitionCount(MinPartitions)); !errors.Is(err, narad.ErrNameTaken) {
			t.Errorf("CreateTopic %s = %v, want ErrNameTaken", upper, err)
			_ = client.DeleteTopic(ctx, upper)
		}
		if _, err := client.EnsureTopic(ctx, upper, narad.WithPartitionCount(MinPartitions)); !errors.Is(err, narad.ErrNameTaken) {
			t.Errorf("EnsureTopic %s = %v, want ErrNameTaken", upper, err)
		}

		long := prefix + "-" + strings.Repeat("x", 201)
		if _, err := client.CreateTopic(ctx, long); !errors.Is(err, narad.ErrBadRequest) {
			t.Errorf("a %d-byte name = %v, want ErrBadRequest", len(long), err)
			_ = client.DeleteTopic(ctx, long)
		}
	})

	t.Run("batch produce and batch consume", func(t *testing.T) {
		name := prefix + "-batch"
		makeTopic(t, client, name, narad.WithPartitionCount(MinPartitions), narad.WithVisibilityTimeout(30*time.Second))

		ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
		defer cancel()

		const count = 30
		keys := map[string]string{} // payment id to the key it was produced with
		var batch narad.Batch
		for i := range count {
			p := payment{ID: fmt.Sprintf("pay_b%02d", i), Amount: int64(i)}
			var opts []narad.ProduceOption
			switch i % 3 {
			case 0:
				keys[p.ID] = fmt.Sprintf("customer-%d", i%4)
			case 1:
				keys[p.ID] = string([]byte{0x00, byte(i), 0x83, 0xff})
			default:
				keys[p.ID] = "" // keyless: the broker sends no key
			}
			if keys[p.ID] != "" {
				opts = append(opts, narad.WithKey(keys[p.ID]))
			}
			if err := batch.Add(p, opts...); err != nil {
				t.Fatalf("Add %d: %v", i, err)
			}
		}
		if err := client.ProduceBatch(ctx, name, &batch); err != nil {
			t.Fatalf("ProduceBatch: %v", err)
		}

		seen := newSeenSet(count)
		var wrongKeys, consumeErrors atomic.Int32
		consumeCtx, stopConsume := context.WithCancel(ctx)
		defer stopConsume()
		go func() {
			_ = client.Consume(consumeCtx, name, narad.HandlerFunc(
				func(_ context.Context, msg *narad.Message) error {
					var got payment
					if err := msg.Into(&got); err != nil {
						return err
					}
					// Counted, not reported here: this goroutine can outlive
					// the subtest, and t must not be used after it ends.
					if msg.Key != keys[got.ID] {
						wrongKeys.Add(1)
					}
					seen.add(got.ID)
					return nil
				}), narad.WithBatch(10), narad.WithWorkers(2), narad.WithWait(5*time.Second),
				// A batch ack the broker refused, or any handle in it, is
				// reported here; none should be.
				narad.WithErrorHandler(func(*narad.Message, error) { consumeErrors.Add(1) }))
		}()

		select {
		case <-seen.done:
		case <-ctx.Done():
			t.Fatalf("consumed %d distinct messages of %d", seen.count(), count)
		}
		stopConsume()
		if wrongKeys.Load() != 0 {
			t.Errorf("%d messages came back with a key other than the one produced", wrongKeys.Load())
		}
		// Give the last batch ack time to land before reading the count.
		time.Sleep(500 * time.Millisecond)
		if consumeErrors.Load() != 0 {
			t.Errorf("the consumer reported %d errors", consumeErrors.Load())
		}
	})

	t.Run("batch produce is all or none", func(t *testing.T) {
		name := prefix + "-batch-schema"
		makeTopic(t, client, name, narad.WithPartitionCount(MinPartitions), narad.WithSchema(json.RawMessage(`{
			"type": "object",
			"properties": {"id": {"type": "string"}, "amount": {"type": "integer"}},
			"required": ["id", "amount"]
		}`)))

		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()

		var batch narad.Batch
		_ = batch.Add(payment{ID: "pay_ok", Amount: 1})
		_ = batch.Add(map[string]any{"id": "pay_bad", "amount": "lots"})
		err := client.ProduceBatch(ctx, name, &batch)
		if !errors.Is(err, narad.ErrBadRequest) {
			t.Fatalf("ProduceBatch with a bad message = %v, want ErrBadRequest", err)
		}
		var apiErr *narad.Error
		if !errors.As(err, &apiErr) || !strings.HasPrefix(apiErr.Message, "message 1: ") {
			t.Errorf("error = %v, want it to name message 1", err)
		}

		// Nothing was stored, the valid message included.
		var stored int
		if err := client.Replay(ctx, name, narad.HandlerFunc(
			func(context.Context, *narad.Message) error { stored++; return nil })); err != nil {
			t.Fatalf("Replay: %v", err)
		}
		if stored != 0 {
			t.Errorf("stored %d messages of a refused batch, want none", stored)
		}
	})

	t.Run("produce and consume", func(t *testing.T) {
		name := prefix + "-basic"
		makeTopic(t, client, name, narad.WithPartitionCount(MinPartitions), narad.WithVisibilityTimeout(30*time.Second))

		ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
		defer cancel()

		const count = 20
		for i := range count {
			err := client.Produce(ctx, name,
				payment{ID: fmt.Sprintf("pay_%02d", i), Amount: int64(i)},
				narad.WithKey(fmt.Sprintf("customer-%d", i%4)))
			if err != nil {
				t.Fatalf("Produce %d: %v", i, err)
			}
		}

		seen := newSeenSet(count)
		consumeCtx, stopConsume := context.WithCancel(ctx)
		defer stopConsume()

		go func() {
			_ = client.Consume(consumeCtx, name, narad.HandlerFunc(
				func(_ context.Context, msg *narad.Message) error {
					var got payment
					if err := msg.Into(&got); err != nil {
						return err
					}
					seen.add(got.ID)
					return nil
				}), narad.WithWorkers(4), narad.WithWait(5*time.Second))
		}()

		select {
		case <-seen.done:
		case <-ctx.Done():
			t.Fatalf("consumed %d distinct messages of %d", seen.count(), count)
		}
		stopConsume()

		if got := seen.count(); got != count {
			t.Errorf("distinct messages = %d, want %d", got, count)
		}
	})

	t.Run("envelope round trip", func(t *testing.T) {
		name := prefix + "-envelope"
		makeTopic(t, client, name, narad.WithPartitionCount(MinPartitions), narad.WithVisibilityTimeout(30*time.Second))

		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()

		err := client.Produce(ctx, name, payment{ID: "pay_env", Amount: 42},
			narad.WithEnvelope(),
			narad.WithHeaders(map[string]string{"trace": "it-trace-1"}))
		if err != nil {
			t.Fatalf("Produce: %v", err)
		}

		msg, err := client.Receive(ctx, name, narad.WithWait(5*time.Second))
		if err != nil {
			t.Fatalf("Receive: %v", err)
		}
		env, err := msg.Envelope()
		if err != nil {
			t.Fatalf("Envelope: %v", err)
		}
		if env.ID == "" || msg.ID() != env.ID {
			t.Errorf("id = %q, msg.ID() = %q", env.ID, msg.ID())
		}
		if env.Attempts != 1 {
			t.Errorf("attempts = %d, want 1", env.Attempts)
		}
		if env.Headers["trace"] != "it-trace-1" {
			t.Errorf("headers = %v", env.Headers)
		}
		if env.Time().IsZero() {
			t.Error("produced_at was not set")
		}
		// Into unwraps, so the caller need not know it was enveloped.
		var got payment
		if err := msg.Into(&got); err != nil {
			t.Fatalf("Into: %v", err)
		}
		if got.ID != "pay_env" || got.Amount != 42 {
			t.Errorf("payload = %+v", got)
		}
		if err := msg.Ack(ctx); err != nil {
			t.Errorf("Ack: %v", err)
		}
	})

	t.Run("retry republishes with a higher attempt count", func(t *testing.T) {
		name := prefix + "-retry"
		makeTopic(t, client, name, narad.WithPartitionCount(MinPartitions), narad.WithVisibilityTimeout(30*time.Second))

		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()

		if err := client.Produce(ctx, name, payment{ID: "pay_retry"}, narad.WithEnvelope()); err != nil {
			t.Fatalf("Produce: %v", err)
		}
		first, err := client.Receive(ctx, name, narad.WithWait(5*time.Second))
		if err != nil {
			t.Fatalf("Receive: %v", err)
		}
		original, err := first.Envelope()
		if err != nil {
			t.Fatalf("Envelope: %v", err)
		}

		if err := client.Retry(ctx, first, errors.New("downstream refused"), ""); err != nil {
			t.Fatalf("Retry: %v", err)
		}

		second, err := client.Receive(ctx, name, narad.WithWait(10*time.Second))
		if err != nil {
			t.Fatalf("Receive the retry: %v", err)
		}
		retried, err := second.Envelope()
		if err != nil {
			t.Fatalf("Envelope of the retry: %v", err)
		}
		if retried.Attempts != original.Attempts+1 {
			t.Errorf("attempts = %d, want %d", retried.Attempts, original.Attempts+1)
		}
		if retried.LastError != "downstream refused" {
			t.Errorf("last error = %q", retried.LastError)
		}
		if retried.ID != original.ID {
			t.Errorf("id changed across the retry: %q then %q", original.ID, retried.ID)
		}
		// The retry is a new record. A keyless message has no key from
		// Narad 3.1.0 on, so its copy is placed round-robin and may land
		// on another partition; on the same one it must come later.
		if second.Partition == first.Partition && second.Offset <= first.Offset {
			t.Errorf("the retry is at %d@%d, want a new record after the original's %d@%d",
				second.Partition, second.Offset, first.Partition, first.Offset)
		}
		if err := second.Ack(ctx); err != nil {
			t.Errorf("Ack: %v", err)
		}
	})

	t.Run("replay reads history back", func(t *testing.T) {
		name := prefix + "-replay"
		makeTopic(t, client, name, narad.WithPartitionCount(MinPartitions), narad.WithVisibilityTimeout(30*time.Second))

		ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
		defer cancel()

		const count = 10
		for i := range count {
			if err := client.Produce(ctx, name, payment{ID: fmt.Sprintf("pay_%02d", i)}); err != nil {
				t.Fatalf("Produce %d: %v", i, err)
			}
		}
		// Give the accepting nodes a moment to commit to the owners.
		time.Sleep(3 * time.Second)

		var replayed atomic.Int32
		err := client.Replay(ctx, name, narad.HandlerFunc(
			func(_ context.Context, msg *narad.Message) error {
				if msg.Leased() {
					t.Error("a replayed message must hold no lease")
				}
				replayed.Add(1)
				return nil
			}))
		if err != nil {
			t.Fatalf("Replay: %v", err)
		}
		if got := replayed.Load(); got != count {
			t.Errorf("replayed %d records, want %d", got, count)
		}

		// A replay settles nothing, so the messages are still queued.
		msg, err := client.Receive(ctx, name, narad.WithWait(5*time.Second))
		if err != nil {
			t.Fatalf("the messages should still be consumable after a replay: %v", err)
		}
		if err := msg.Ack(ctx); err != nil {
			t.Errorf("Ack: %v", err)
		}
	})

	t.Run("envelope schema is enforced", func(t *testing.T) {
		name := prefix + "-schema"
		schema := json.RawMessage(`{
			"type": "object",
			"properties": {
				"id":     {"type": "string"},
				"amount": {"type": "integer", "minimum": 0}
			},
			"required": ["id", "amount"],
			"additionalProperties": false
		}`)
		makeTopic(t, client, name,
			narad.WithPartitionCount(MinPartitions),
			narad.WithVisibilityTimeout(30*time.Second),
			narad.WithEnvelopeSchema(schema))

		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()

		// An enveloped message that fits is accepted.
		if err := client.Produce(ctx, name, payment{ID: "pay_ok", Amount: 1}, narad.WithEnvelope()); err != nil {
			t.Fatalf("a valid enveloped message was rejected: %v", err)
		}
		// One that does not fit is refused by the broker, inside the
		// envelope, which is the whole point of wrapping the schema.
		bad := map[string]any{"id": "pay_bad", "amount": "not a number"}
		err := client.Produce(ctx, name, bad, narad.WithEnvelope())
		if !errors.Is(err, narad.ErrBadRequest) {
			t.Errorf("an invalid message was accepted, or failed oddly: %v", err)
		}
		// And a message with no envelope fails against an envelope
		// schema, which the documentation promises.
		if err := client.Produce(ctx, name, payment{ID: "pay_plain", Amount: 1}); !errors.Is(err, narad.ErrBadRequest) {
			t.Errorf("a plain message was accepted by an envelope schema: %v", err)
		}

		msg, err := client.Receive(ctx, name, narad.WithWait(5*time.Second))
		if err != nil {
			t.Fatalf("Receive: %v", err)
		}
		var got payment
		if err := msg.Into(&got); err != nil || got.ID != "pay_ok" {
			t.Errorf("payload = %+v, err %v", got, err)
		}
		if err := msg.Ack(ctx); err != nil {
			t.Errorf("Ack: %v", err)
		}
	})

	t.Run("errors are mapped", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		missing := prefix + "-does-not-exist"
		if err := client.Produce(ctx, missing, payment{ID: "x"}); !errors.Is(err, narad.ErrNotFound) {
			t.Errorf("produce to a missing topic = %v, want ErrNotFound", err)
		}
		if _, err := client.Topic(ctx, missing); !errors.Is(err, narad.ErrNotFound) {
			t.Errorf("describe a missing topic = %v, want ErrNotFound", err)
		}
		// A message too large is caught before it leaves.
		if err := client.Produce(ctx, missing, make([]byte, narad.MaxMessageBytes+1)); !errors.Is(err, narad.ErrTooLarge) {
			t.Errorf("oversize = %v, want ErrTooLarge", err)
		}

		// Wrong credentials must read as authentication, not as
		// something ambiguous.
		bad, err := narad.New(os.Getenv("NARAD_ADDR"),
			narad.WithAuth("admin", "definitely-not-the-password"),
			narad.WithRetries(1))
		if err != nil {
			t.Fatalf("New: %v", err)
		}
		defer bad.Close()
		if _, err := bad.Topics(ctx); !errors.Is(err, narad.ErrUnauthenticated) && !errors.Is(err, narad.ErrThrottled) {
			// The broker throttles repeated bad passwords, so either
			// answer is correct.
			t.Errorf("bad credentials = %v, want ErrUnauthenticated or ErrThrottled", err)
		}
	})

	t.Run("topics lists our own", func(t *testing.T) {
		// This subtest makes its own topic rather than looking for the
		// earlier ones: each subtest's t.Cleanup deletes its topics when
		// that subtest ends, so by now they are gone. A first version of
		// this test looked for them and failed for that reason alone.
		names := []string{prefix + "-listed-a", prefix + "-listed-b"}
		for _, name := range names {
			makeTopic(t, client, name, narad.WithPartitionCount(MinPartitions))
		}

		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()

		// On a shared cluster this also follows the pagination to the
		// end; a fresh local node may hold only these two.
		topics, err := client.Topics(ctx)
		if err != nil {
			t.Fatalf("Topics: %v", err)
		}
		listed := map[string]bool{}
		for _, topic := range topics {
			listed[topic.Name] = true
		}
		for _, name := range names {
			if !listed[name] {
				t.Errorf("%s was created but did not appear in the listing of %d topics", name, len(topics))
			}
		}
	})
}

// seenSet records which message ids a consumer has handled, and signals
// once the expected number have arrived.
type seenSet struct {
	mu   sync.Mutex
	ids  map[string]bool
	want int
	done chan struct{}
	once sync.Once
}

func newSeenSet(want int) *seenSet {
	return &seenSet{ids: make(map[string]bool, want), want: want, done: make(chan struct{})}
}

func (s *seenSet) add(id string) {
	s.mu.Lock()
	s.ids[id] = true
	enough := len(s.ids) >= s.want
	s.mu.Unlock()
	if enough {
		s.once.Do(func() { close(s.done) })
	}
}

func (s *seenSet) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.ids)
}
