package narad

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"sync/atomic"
	"testing"
	"time"
)

// The fixture is the broker's own wire shape. Writing the field names
// the client happens to expect would make the test confirm the struct
// against itself, and every field would read back zero against a real
// broker.
const topicFixture = `{
	"name": "orders",
	"partitions": 3,
	"retention_ms": 86400000,
	"visibility_timeout_ms": 30000,
	"created_at": 1700000000000,
	"owner": "svc",
	"children": ["orders-audit"],
	"schema_version": 2,
	"partition_stats": [
		{"index":0,"segments":3,"oldest_offset":100,"next_offset":450,
		 "high_watermark":450,"size_bytes":8192,"owner_node":"narad-1"}
	]
}`

func TestCreateAndDescribeTopic(t *testing.T) {
	t.Parallel()

	var sent []byte
	c := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			sent, _ = io.ReadAll(r.Body)
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusCreated)
		} else {
			w.Header().Set("Content-Type", "application/json")
		}
		_, _ = w.Write([]byte(topicFixture))
	})

	ctx := context.Background()
	if _, err := c.CreateTopic(ctx, "orders",
		WithPartitionCount(3), WithRetention(24*time.Hour)); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}

	var body map[string]any
	if err := json.Unmarshal(sent, &body); err != nil {
		t.Fatalf("request body: %v", err)
	}
	if body["name"] != "orders" || body["partitions"] != float64(3) {
		t.Errorf("request = %s", sent)
	}
	if body["retention_ms"] != float64(86_400_000) {
		t.Errorf("retention = %v", body["retention_ms"])
	}
	// Anything not set must not be sent, or the broker is told to use
	// zero rather than its own default.
	for _, absent := range []string{"visibility_timeout_ms", "max_in_flight_per_partition", "schema", "parent"} {
		if _, present := body[absent]; present {
			t.Errorf("%q was sent although it was never set", absent)
		}
	}

	topic, err := c.Topic(ctx, "orders")
	if err != nil {
		t.Fatalf("Topic: %v", err)
	}
	if topic.Retention != 24*time.Hour {
		t.Errorf("retention = %s", topic.Retention)
	}
	if topic.VisibilityTimeout != 30*time.Second {
		t.Errorf("visibility = %s", topic.VisibilityTimeout)
	}
	if topic.SchemaVersion != 2 {
		t.Errorf("schema version = %d, want 2: it is needed to update a schema safely", topic.SchemaVersion)
	}
	if topic.Owner != "svc" || len(topic.Children) != 1 {
		t.Errorf("topic = %+v", topic)
	}
	if topic.Created.IsZero() {
		t.Error("created_at did not decode")
	}
	stats := topic.PartitionStats
	if len(stats) != 1 {
		t.Fatalf("partition stats = %+v", stats)
	}
	if stats[0].Committed != 450 || stats[0].Oldest != 100 || stats[0].Bytes != 8192 {
		t.Errorf("partition stats did not decode: %+v", stats[0])
	}
	if stats[0].Owner != "narad-1" {
		t.Errorf("owner node = %q", stats[0].Owner)
	}
	if got := stats[0].Depth(); got != 350 {
		t.Errorf("Depth() = %d, want 350", got)
	}
	if got := (PartitionStats{Oldest: 5, Committed: 5}).Depth(); got != 0 {
		t.Errorf("empty partition Depth() = %d, want 0", got)
	}
}

// Every replica of a service racing to create the same topics at startup
// is the normal case, and only one can win.
func TestEnsureTopicAcceptsOneThatExists(t *testing.T) {
	t.Parallel()

	c := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.Method == http.MethodPost {
			w.WriteHeader(http.StatusConflict)
			_, _ = w.Write([]byte(`{"error":"already exists"}`))
			return
		}
		_, _ = w.Write([]byte(topicFixture))
	})

	topic, err := c.EnsureTopic(context.Background(), "orders", WithPartitionCount(6))
	if err != nil {
		t.Fatalf("EnsureTopic: %v", err)
	}
	// What is actually running wins over what was asked for.
	if topic.Partitions != 3 {
		t.Errorf("partitions = %d, want the existing topic's 3", topic.Partitions)
	}
}

func TestEnsureTopicPassesOnRealFailures(t *testing.T) {
	t.Parallel()

	c := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusForbidden)
		_, _ = w.Write([]byte(`{"error":"no create grant"}`))
	})
	if _, err := c.EnsureTopic(context.Background(), "orders"); !errors.Is(err, ErrForbidden) {
		t.Fatalf("err = %v, want ErrForbidden", err)
	}
}

func TestTopicsFollowsPages(t *testing.T) {
	t.Parallel()

	var calls atomic.Int32
	c := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if calls.Add(1) == 1 {
			if got := r.URL.Query().Get("page_token"); got != "" {
				t.Errorf("first page sent a token: %q", got)
			}
			_, _ = w.Write([]byte(`{"topics":[{"name":"a"},{"name":"b"}],"next_page_token":"tok"}`))
			return
		}
		if got := r.URL.Query().Get("page_token"); got != "tok" {
			t.Errorf("second page token = %q", got)
		}
		_, _ = w.Write([]byte(`{"topics":[{"name":"c"}],"next_page_token":""}`))
	})

	topics, err := c.Topics(context.Background())
	if err != nil {
		t.Fatalf("Topics: %v", err)
	}
	if len(topics) != 3 {
		t.Fatalf("topics = %d, want 3 across both pages", len(topics))
	}
}

// Deleting something already gone is the outcome the caller wanted, and
// a 404 here is usually a retry whose first attempt worked.
func TestDeleteTopicIsIdempotent(t *testing.T) {
	t.Parallel()

	c := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte(`{"error":"topic not found"}`))
	})
	if err := c.DeleteTopic(context.Background(), "gone"); err != nil {
		t.Errorf("DeleteTopic on a missing topic = %v, want nil", err)
	}
}

func TestSetSchemaSendsTheWrappedSchemaAndBaseVersion(t *testing.T) {
	t.Parallel()

	var sent []byte
	c := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPatch {
			sent, _ = io.ReadAll(r.Body)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(topicFixture))
	})

	user := json.RawMessage(`{"type":"object","properties":{"id":{"type":"string"}}}`)
	if _, err := c.SetSchema(context.Background(), "orders",
		WithEnvelopeSchema(user), WithSchemaBaseVersion(2)); err != nil {
		t.Fatalf("SetSchema: %v", err)
	}

	var body struct {
		Schema  map[string]any `json:"schema"`
		BaseVer int            `json:"schema_base_version"`
	}
	if err := json.Unmarshal(sent, &body); err != nil {
		t.Fatalf("request body: %v", err)
	}
	if body.BaseVer != 2 {
		t.Errorf("base version = %d, want 2", body.BaseVer)
	}
	props, _ := body.Schema["properties"].(map[string]any)
	if _, ok := props["body"]; !ok {
		t.Errorf("the schema sent was not wrapped: %s", sent)
	}
	if _, ok := props["_narad"]; !ok {
		t.Error("the wrapped schema does not describe the envelope marker")
	}
}

func TestSetSchemaNeedsASchema(t *testing.T) {
	t.Parallel()

	c := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		t.Error("nothing should have been sent")
	})
	if _, err := c.SetSchema(context.Background(), "orders"); !errors.Is(err, ErrBadRequest) {
		t.Errorf("no schema: %v", err)
	}
	if _, err := c.SetSchema(context.Background(), ""); !errors.Is(err, ErrBadRequest) {
		t.Errorf("no name: %v", err)
	}
}

func TestTopicCallsRejectEmptyNames(t *testing.T) {
	t.Parallel()

	c := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		t.Error("nothing should have been sent")
	})
	ctx := context.Background()
	checks := map[string]error{
		"create": errFrom(func() error { _, err := c.CreateTopic(ctx, ""); return err }),
		"topic":  errFrom(func() error { _, err := c.Topic(ctx, ""); return err }),
		"delete": c.DeleteTopic(ctx, ""),
		"read":   errFrom(func() error { _, err := c.ReadAt(ctx, "", 0, 0); return err }),
	}
	for name, err := range checks {
		if !errors.Is(err, ErrBadRequest) {
			t.Errorf("%s: err = %v, want ErrBadRequest without a round trip", name, err)
		}
	}
	if _, err := c.ReadAt(ctx, "orders", 0, -1); !errors.Is(err, ErrBadRequest) {
		t.Errorf("negative offset: %v", err)
	}
}

func errFrom(fn func() error) error { return fn() }
