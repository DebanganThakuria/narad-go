package narad

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"time"
)

// A Topic describes a topic as the broker reports it.
type Topic struct {
	Name       string
	Partitions int
	// Retention is how long records are kept.
	Retention time.Duration
	// VisibilityTimeout is how long a consumer holds a message before it
	// is redelivered.
	VisibilityTimeout time.Duration
	// Owner is the user that created it, empty when security is off.
	Owner string
	// Parent names the topic this one fans out from, empty unless it is
	// a child.
	Parent string
	// Children names the topics this one fans out to.
	Children []string
	// Delay is how long a delay child holds each copy back.
	Delay time.Duration
	// Schema is the JSON Schema the broker enforces on produce, absent
	// when the topic has none.
	Schema json.RawMessage
	// SchemaVersion is the version currently in force, 0 when there is
	// no schema. Pass it to [Client.SetSchema] to update without
	// clobbering a concurrent change.
	SchemaVersion int
	// Created is when the topic was created.
	Created time.Time
	// Partitions detail, one per partition.
	PartitionStats []PartitionStats
}

// PartitionStats is one partition's storage state.
type PartitionStats struct {
	Index    int   `json:"index"`
	Segments int   `json:"segments"`
	Bytes    int64 `json:"size_bytes"`
	// Oldest is the lowest offset still on disk. Anything below it has
	// aged out of retention.
	Oldest int64 `json:"oldest_offset"`
	// Next is the offset the next record will get.
	Next int64 `json:"next_offset"`
	// Committed is the exclusive upper bound of what consumers can see.
	Committed int64 `json:"high_watermark"`
	// Owner is the node whose disk holds this partition.
	Owner string `json:"owner_node"`
}

// Depth is how many records the partition still holds: the committed
// frontier less what has aged out.
//
// It is not a backlog. A record in that range may already have been
// consumed and acked, because Narad tracks progress with leases rather
// than a cursor. For a real backlog, read the broker's lag metrics.
func (p PartitionStats) Depth() int64 {
	if p.Committed <= p.Oldest {
		return 0
	}
	return p.Committed - p.Oldest
}

// A TopicOption adjusts a topic being created or changed.
type TopicOption func(*topicConfig)

type topicConfig struct {
	partitions int
	retention  time.Duration
	visibility time.Duration
	inFlight   int64
	ackedAhead int64
	schema     json.RawMessage
	parent     string
	delay      time.Duration
	baseVer    int
}

// WithPartitionCount sets how many partitions the topic has. More
// partitions means more consumers can work at once. The broker picks a
// default if you do not.
func WithPartitionCount(n int) TopicOption {
	return func(c *topicConfig) { c.partitions = n }
}

// WithRetention sets how long records are kept. The broker enforces a
// floor of one hour.
func WithRetention(d time.Duration) TopicOption {
	return func(c *topicConfig) { c.retention = d }
}

// WithVisibilityTimeout sets how long a consumer holds a message before
// it is handed to somebody else.
//
// Set it a little above how long handling actually takes.
// [Client.Consume] renews the lease for slow work, so this is a backstop
// rather than a deadline.
func WithVisibilityTimeout(d time.Duration) TopicOption {
	return func(c *topicConfig) { c.visibility = d }
}

// WithMaxInFlight caps how many messages may be unacked at once per
// partition.
func WithMaxInFlight(n int64) TopicOption {
	return func(c *topicConfig) { c.inFlight = n }
}

// WithSchema makes the broker validate every message against this JSON
// Schema and reject the ones that do not fit, before they reach the log.
//
// The schema describes the message itself, so producing with
// [WithEnvelope] to such a topic fails. Use [WithEnvelopeSchema] when
// you want both.
func WithSchema(schema json.RawMessage) TopicOption {
	return func(c *topicConfig) { c.schema = schema }
}

// WithEnvelopeSchema validates enveloped messages, checking the given
// schema against the message inside the envelope.
//
// This is how a topic gets both: producers use [WithEnvelope], the
// broker still rejects a malformed message, and the envelope's own
// fields are checked too. The schema stored on the topic is the wrapped
// one, which [EnvelopeSchema] will show you.
//
// Producers must then always use an envelope. A plain message will not
// validate against it.
func WithEnvelopeSchema(schema json.RawMessage) TopicOption {
	return func(c *topicConfig) {
		wrapped, err := EnvelopeSchema(schema)
		if err != nil {
			// Reported when the option is applied, so the caller sees it
			// from CreateTopic rather than as a puzzling 400.
			c.schema = json.RawMessage(fmt.Sprintf(`{"narad_schema_error":%q}`, err.Error()))
			return
		}
		c.schema = wrapped
	}
}

// WithParent makes the new topic a fan-out child of another: every
// message produced to the parent is copied to it.
func WithParent(parent string) TopicOption {
	return func(c *topicConfig) { c.parent = parent }
}

// WithDelay holds each copy back by d on a fan-out child, which is how
// Narad schedules work for later. It only means anything alongside
// [WithParent].
func WithDelay(d time.Duration) TopicOption {
	return func(c *topicConfig) { c.delay = d }
}

// WithSchemaBaseVersion makes a schema change conditional on that
// version still being current, so two writers cannot silently overwrite
// each other. A mismatch reports [ErrExists]: read the topic again and
// retry against the version you find.
func WithSchemaBaseVersion(version int) TopicOption {
	return func(c *topicConfig) { c.baseVer = version }
}

// topicJSON is the wire shape of a topic record.
type topicJSON struct {
	Name                string           `json:"name"`
	Partitions          int              `json:"partitions"`
	RetentionMs         int64            `json:"retention_ms"`
	VisibilityTimeoutMs int64            `json:"visibility_timeout_ms"`
	MaxInFlight         int64            `json:"max_in_flight_per_partition"`
	MaxAckedAhead       int64            `json:"max_acked_ahead_per_partition"`
	CreatedAt           int64            `json:"created_at"`
	Owner               string           `json:"owner,omitempty"`
	Parent              string           `json:"parent,omitempty"`
	Children            []string         `json:"children,omitempty"`
	FanoutDelayMs       int64            `json:"fanout_delay_ms,omitempty"`
	Schema              json.RawMessage  `json:"schema,omitempty"`
	SchemaVersion       int              `json:"schema_version,omitempty"`
	PartitionStats      []PartitionStats `json:"partition_stats,omitempty"`
}

func (t topicJSON) toTopic() Topic {
	out := Topic{
		Name:              t.Name,
		Partitions:        t.Partitions,
		Retention:         time.Duration(t.RetentionMs) * time.Millisecond,
		VisibilityTimeout: time.Duration(t.VisibilityTimeoutMs) * time.Millisecond,
		Owner:             t.Owner,
		Parent:            t.Parent,
		Children:          t.Children,
		Delay:             time.Duration(t.FanoutDelayMs) * time.Millisecond,
		Schema:            t.Schema,
		SchemaVersion:     t.SchemaVersion,
		PartitionStats:    t.PartitionStats,
	}
	if t.CreatedAt != 0 {
		out.Created = time.UnixMilli(t.CreatedAt)
	}
	return out
}

// CreateTopic creates a topic.
//
//	_, err := client.CreateTopic(ctx, "orders",
//		narad.WithPartitionCount(6),
//		narad.WithRetention(24*time.Hour))
//
// Creating one that already exists reports [ErrExists]. When that is not
// an error for you, which it usually is not, use [Client.EnsureTopic].
func (c *Client) CreateTopic(ctx context.Context, name string, opts ...TopicOption) (Topic, error) {
	var out Topic
	if name == "" {
		return out, fmt.Errorf("narad: create topic: %w: name is required", ErrBadRequest)
	}
	var cfg topicConfig
	for _, opt := range opts {
		opt(&cfg)
	}
	if err := cfg.schemaError(); err != nil {
		return out, fmt.Errorf("narad: create topic %s: %w", name, err)
	}

	body, err := json.Marshal(struct {
		Name                string          `json:"name"`
		Partitions          int             `json:"partitions,omitempty"`
		RetentionMs         int64           `json:"retention_ms,omitempty"`
		VisibilityTimeoutMs int64           `json:"visibility_timeout_ms,omitempty"`
		MaxInFlight         int64           `json:"max_in_flight_per_partition,omitempty"`
		MaxAckedAhead       int64           `json:"max_acked_ahead_per_partition,omitempty"`
		Schema              json.RawMessage `json:"schema,omitempty"`
		Parent              string          `json:"parent,omitempty"`
		FanoutDelayMs       int64           `json:"fanout_delay_ms,omitempty"`
	}{
		Name:                name,
		Partitions:          cfg.partitions,
		RetentionMs:         cfg.retention.Milliseconds(),
		VisibilityTimeoutMs: cfg.visibility.Milliseconds(),
		MaxInFlight:         cfg.inFlight,
		MaxAckedAhead:       cfg.ackedAhead,
		Schema:              cfg.schema,
		Parent:              cfg.parent,
		FanoutDelayMs:       cfg.delay.Milliseconds(),
	})
	if err != nil {
		return out, fmt.Errorf("narad: create topic %s: encode request: %w", name, err)
	}

	res, err := c.do(ctx, call{
		method:      http.MethodPost,
		path:        "/v1/topics",
		body:        body,
		contentType: "application/json",
		op:          "create topic",
		topic:       name,
		ok:          []int{http.StatusCreated},
	})
	if err != nil {
		return out, err
	}
	var record topicJSON
	if err := decode(res.body, &record, "create topic"); err != nil {
		return out, err
	}
	return record.toTopic(), nil
}

// EnsureTopic creates a topic, treating one that already exists as
// success and returning what is actually there.
//
// This is what a service starting up wants: every replica of it races
// every other to create the same topics, and only one can win.
func (c *Client) EnsureTopic(ctx context.Context, name string, opts ...TopicOption) (Topic, error) {
	created, err := c.CreateTopic(ctx, name, opts...)
	if err == nil {
		return created, nil
	}
	if !errorIs(err, ErrExists) {
		return created, err
	}
	existing, lookupErr := c.Topic(ctx, name)
	if lookupErr != nil {
		return created, err
	}
	return existing, nil
}

// Topic describes one topic, including its per-partition state.
func (c *Client) Topic(ctx context.Context, name string) (Topic, error) {
	var out Topic
	if name == "" {
		return out, fmt.Errorf("narad: topic: %w: name is required", ErrBadRequest)
	}
	res, err := c.do(ctx, call{
		method: http.MethodGet,
		path:   "/v1/topics/" + url.PathEscape(name),
		op:     "topic",
		topic:  name,
		ok:     []int{http.StatusOK},
	})
	if err != nil {
		return out, err
	}
	var record topicJSON
	if err := decode(res.body, &record, "topic"); err != nil {
		return out, err
	}
	return record.toTopic(), nil
}

// Topics lists every topic the caller may see, following pagination.
//
// The result is filtered by grant, so an empty list can mean there are
// none or that you may see none.
func (c *Client) Topics(ctx context.Context) ([]Topic, error) {
	var all []Topic
	token := ""
	for {
		query := url.Values{}
		if token != "" {
			query.Set("page_token", token)
		}
		res, err := c.do(ctx, call{
			method: http.MethodGet,
			path:   "/v1/topics",
			query:  query.Encode(),
			op:     "topics",
			ok:     []int{http.StatusOK},
		})
		if err != nil {
			return nil, err
		}
		var page struct {
			Topics []topicJSON `json:"topics"`
			Next   string      `json:"next_page_token"`
		}
		if err := decode(res.body, &page, "topics"); err != nil {
			return nil, err
		}
		for _, record := range page.Topics {
			all = append(all, record.toTopic())
		}
		if page.Next == "" {
			return all, nil
		}
		token = page.Next
	}
}

// DeleteTopic removes a topic and everything in it. This cannot be
// undone.
//
// A topic that is already gone counts as deleted: the usual way to see
// that is a retry whose first attempt actually worked.
func (c *Client) DeleteTopic(ctx context.Context, name string) error {
	if name == "" {
		return fmt.Errorf("narad: delete topic: %w: name is required", ErrBadRequest)
	}
	_, err := c.do(ctx, call{
		method: http.MethodDelete,
		path:   "/v1/topics/" + url.PathEscape(name),
		op:     "delete topic",
		topic:  name,
		ok:     []int{http.StatusNoContent, http.StatusNotFound},
	})
	return err
}

// SetSchema registers a new schema version for a topic.
//
// Versions are append-only and checked for compatibility, so a consumer
// written against an older one keeps working. Use [WithSchema] or
// [WithEnvelopeSchema] to supply it, and [WithSchemaBaseVersion] to make
// the change conditional on the version you read.
//
//	topic, _ := client.Topic(ctx, "orders")
//	_, err := client.SetSchema(ctx, "orders",
//		narad.WithEnvelopeSchema(schema),
//		narad.WithSchemaBaseVersion(topic.SchemaVersion))
func (c *Client) SetSchema(ctx context.Context, name string, opts ...TopicOption) (Topic, error) {
	var out Topic
	if name == "" {
		return out, fmt.Errorf("narad: set schema: %w: name is required", ErrBadRequest)
	}
	var cfg topicConfig
	for _, opt := range opts {
		opt(&cfg)
	}
	if err := cfg.schemaError(); err != nil {
		return out, fmt.Errorf("narad: set schema %s: %w", name, err)
	}
	if len(cfg.schema) == 0 {
		return out, fmt.Errorf("narad: set schema %s: %w: no schema given", name, ErrBadRequest)
	}

	body, err := json.Marshal(struct {
		Schema  json.RawMessage `json:"schema"`
		BaseVer int             `json:"schema_base_version,omitempty"`
	}{cfg.schema, cfg.baseVer})
	if err != nil {
		return out, fmt.Errorf("narad: set schema %s: encode request: %w", name, err)
	}

	res, err := c.do(ctx, call{
		method:      http.MethodPatch,
		path:        "/v1/topics/" + url.PathEscape(name),
		body:        body,
		contentType: "application/json",
		op:          "set schema",
		topic:       name,
		ok:          []int{http.StatusOK},
	})
	if err != nil {
		return out, err
	}
	var record topicJSON
	if err := decode(res.body, &record, "set schema"); err != nil {
		return out, err
	}
	return record.toTopic(), nil
}

// schemaError reports a schema that failed to wrap when the option was
// applied.
func (c topicConfig) schemaError() error {
	if len(c.schema) == 0 {
		return nil
	}
	var probe struct {
		Err string `json:"narad_schema_error"`
	}
	if json.Unmarshal(c.schema, &probe) == nil && probe.Err != "" {
		return fmt.Errorf("%w: %s", ErrBadRequest, probe.Err)
	}
	return nil
}

// EnvelopeSchema wraps a schema for the message in a schema for the
// envelope around it, so a topic can enforce both at once.
//
// The result validates the envelope's own fields and checks the given
// schema against the message in its body. [WithEnvelopeSchema] applies
// it for you; this is exported so you can see what will be stored, or
// register it with other tooling.
//
// One caveat about references. Any $defs or definitions in the schema
// are lifted to the top of the result, so an internal "#/$defs/..."
// reference still resolves. A reference to the document root itself
// ("#") will not, because the root is now the envelope.
func EnvelopeSchema(schema json.RawMessage) (json.RawMessage, error) {
	if len(bytes.TrimSpace(schema)) == 0 {
		return nil, fmt.Errorf("schema is empty")
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(schema, &fields); err != nil {
		return nil, fmt.Errorf("schema is not a JSON object: %w", err)
	}

	// Lift the definitions to the root and drop identity keywords, which
	// would otherwise re-anchor reference resolution inside the body.
	defs := fields["$defs"]
	legacyDefs := fields["definitions"]
	dialect := fields["$schema"]
	delete(fields, "$defs")
	delete(fields, "definitions")
	delete(fields, "$schema")
	delete(fields, "$id")

	body, err := json.Marshal(fields)
	if err != nil {
		return nil, fmt.Errorf("re-encode schema: %w", err)
	}

	root := map[string]json.RawMessage{
		"type": json.RawMessage(`"object"`),
		"properties": json.RawMessage(`{
			"_narad":      {"type": "integer"},
			"id":          {"type": "string", "minLength": 1},
			"produced_at": {"type": "integer"},
			"attempts":    {"type": "integer", "minimum": 1},
			"last_error":  {"type": "string"},
			"headers":     {"type": "object", "additionalProperties": {"type": "string"}},
			"body":        ` + string(body) + `
		}`),
		"required":             json.RawMessage(`["_narad", "id", "body", "produced_at"]`),
		"additionalProperties": json.RawMessage(`false`),
	}
	if len(dialect) > 0 {
		root["$schema"] = dialect
	}
	if len(defs) > 0 {
		root["$defs"] = defs
	}
	if len(legacyDefs) > 0 {
		root["definitions"] = legacyDefs
	}

	out, err := json.Marshal(root)
	if err != nil {
		return nil, fmt.Errorf("encode envelope schema: %w", err)
	}
	return out, nil
}
