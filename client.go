package narad

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
)

// Version is this client's version, sent in the User-Agent.
const Version = "0.1.0"

// MaxMessageBytes is the largest message the broker accepts.
const MaxMessageBytes = 1 << 20

// Operation names, used in errors and in the event stream.
const (
	opProduce = "produce"
	opConsume = "consume"
	opRead    = "read"
	opAck     = "ack"
	opNack    = "nack"
	opExtend  = "extend"
)

const (
	userAgent = "narad-go/" + Version
	// maxErrorBody caps how much of a failed reply is read. The server's
	// errors are one sentence; anything longer is a proxy's HTML page.
	maxErrorBody = 8 << 10
)

// A Client talks to a Narad cluster.
//
// It is safe for concurrent use and meant to be long lived: it owns the
// connection pool and the per-node circuit breakers, so one created per
// request throws both away.
type Client struct {
	nodes  *nodeSet
	http   *http.Client
	cfg    config
	closed atomic.Bool
	// ownHTTP records whether Close may shut the transport down. A
	// caller who supplied their own client keeps control of it.
	ownHTTP bool
}

// New connects to a Narad cluster.
//
// The address is one or more node addresses, comma separated. The scheme
// defaults to http, so "localhost:7942" and
// "n1:7942,n2:7942,n3:7942" both work.
//
// Give it every node you have. The client spreads work across them and
// routes around the ones that are failing, which is what makes a node
// restart invisible. One address works too, and then the load balancer
// in front, rather than this client, decides what to do about a dead
// node.
func New(address string, opts ...Option) (*Client, error) {
	addresses, err := parseAddresses(address)
	if err != nil {
		return nil, err
	}

	cfg := defaults()
	for _, opt := range opts {
		opt(&cfg)
	}

	c := &Client{cfg: cfg}
	if cfg.http != nil {
		c.http = cfg.http
	} else {
		c.http = &http.Client{Transport: newTransport(cfg)}
		c.ownHTTP = true
	}
	c.nodes = newNodeSet(addresses, cfg)
	return c, nil
}

// Close releases the client's connections. It may be called more than
// once. The client cannot be used afterwards.
func (c *Client) Close() error {
	if !c.closed.CompareAndSwap(false, true) {
		return nil
	}
	if c.ownHTTP {
		if t, ok := c.http.Transport.(*http.Transport); ok {
			t.CloseIdleConnections()
		}
	}
	return nil
}

// Nodes returns the addresses the client was given.
func (c *Client) Nodes() []string {
	out := make([]string, 0, len(c.nodes.all))
	for _, n := range c.nodes.all {
		out = append(out, n.address)
	}
	return out
}

// Health reports what the client currently believes about each node.
//
// It is derived from the requests already made rather than from a probe,
// so a node nothing has been sent to reads as healthy. To ask a node
// directly, use [Client.Ping].
func (c *Client) Health() []NodeHealth { return c.nodes.health() }

// NodeHealth is one node's state as the client sees it.
type NodeHealth struct {
	// Address is the node's base URL.
	Address string
	// Healthy is true while the client is sending it requests.
	Healthy bool
	// State is "closed", "open" or "half-open".
	State string
	// LastError is the failure that last counted against it, nil when
	// healthy.
	LastError error
}

// newTransport builds the HTTP transport.
//
// There is deliberately no Timeout on the http.Client: a long-polling
// consume can take tens of seconds, and a client-wide timeout would
// cancel it. Deadlines come from each request's context instead, which
// lets an ordinary call be impatient while a poll is not.
func newTransport(cfg config) *http.Transport {
	t := http.DefaultTransport.(*http.Transport).Clone()
	t.MaxIdleConnsPerHost = cfg.maxIdleConns
	t.IdleConnTimeout = 90 * time.Second
	if t.MaxIdleConns < cfg.maxIdleConns {
		t.MaxIdleConns = cfg.maxIdleConns * 4
	}
	return t
}

// parseAddresses splits a comma-separated address list and normalizes
// each entry to a bare origin.
func parseAddresses(address string) ([]string, error) {
	parts := strings.Split(address, ",")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		if !strings.Contains(part, "://") {
			part = "http://" + part
		}
		part = strings.TrimRight(part, "/")
		if !strings.HasPrefix(part, "http://") && !strings.HasPrefix(part, "https://") {
			return nil, fmt.Errorf("narad: %w: address %q must be http or https", ErrBadRequest, part)
		}
		out = append(out, part)
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("narad: %w: no address given", ErrBadRequest)
	}
	return out, nil
}

// call is one logical operation, which may take several HTTP attempts.
type call struct {
	method      string
	path        string
	query       string
	body        []byte
	contentType string
	op          string
	topic       string
	// ok lists the statuses that count as success.
	ok []int
	// timeout overrides the configured per-attempt timeout. Consume uses
	// it so a long poll outlives an ordinary request.
	timeout time.Duration
	// once disables retries, for callers running their own loop.
	once bool
}

// reply is a successful response.
type reply struct {
	status int
	body   []byte
	header http.Header
	node   string
}

// do runs a call, retrying across nodes under the retry policy.
func (c *Client) do(ctx context.Context, rc call) (*reply, error) {
	if c.closed.Load() {
		return nil, ErrClosed
	}
	attempts := c.cfg.attempts
	if attempts < 1 || rc.once {
		attempts = 1
	}

	tried := make(map[string]bool, len(c.nodes.all))
	var last error

	for attempt := range attempts {
		node := c.nodes.pick(tried)
		if node == nil {
			if last != nil {
				return nil, last
			}
			return nil, ErrNoNodes
		}
		tried[node.address] = true

		res, err := c.attempt(ctx, node, rc, attempt)
		node.observe(err)
		if err == nil {
			return res, nil
		}
		last = err

		if attempt == attempts-1 || !Retryable(err) {
			break
		}
		// An uncertain failure on something that changes state is the
		// one case where retrying can do harm rather than waste time.
		// The caller decided in advance.
		if c.cfg.cautious && Uncertain(err) {
			break
		}
		if ctx.Err() != nil {
			break
		}

		delay := c.delay(err, attempt)
		c.emit(Event{
			Kind: EventRetry, Op: rc.op, Topic: rc.topic, Node: node.address,
			Attempt: attempt, Wait: delay, Err: err, Uncertain: Uncertain(err),
		})
		c.logger().Warn("narad: retrying",
			"op", rc.op, "topic", rc.topic, "node", node.address,
			"attempt", attempt+1, "wait", delay, "uncertain", Uncertain(err), "err", err)
		if !wait(ctx, delay) {
			return nil, ctx.Err()
		}
	}
	return nil, last
}

// delay honours the server's Retry-After when it sent one: a broker
// shedding load knows how long it needs better than any local curve.
func (c *Client) delay(err error, attempt int) time.Duration {
	var apiErr *Error
	if errors.As(err, &apiErr) && apiErr.RetryAfter > 0 {
		return apiErr.RetryAfter
	}
	return c.cfg.backoff.delay(attempt)
}

// attempt performs one HTTP request.
func (c *Client) attempt(ctx context.Context, node *node, rc call, attempt int) (*reply, error) {
	timeout := rc.timeout
	if timeout <= 0 {
		timeout = c.cfg.timeout
	}
	if timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}

	url := node.address + rc.path
	if rc.query != "" {
		url += "?" + rc.query
	}

	var body io.Reader
	if rc.body != nil {
		body = bytes.NewReader(rc.body)
	}
	req, err := http.NewRequestWithContext(ctx, rc.method, url, body)
	if err != nil {
		return nil, &ConnError{Op: rc.op, Topic: rc.topic, Node: node.address, Err: err}
	}
	if rc.body != nil {
		req.ContentLength = int64(len(rc.body))
		raw := rc.body
		req.GetBody = func() (io.ReadCloser, error) {
			return io.NopCloser(bytes.NewReader(raw)), nil
		}
	}
	// Set outside the body check. A body-less POST still has to satisfy
	// the server's cross-site guard, and tying the header to the body
	// leaves such a request declaring a type it never sends.
	if rc.contentType != "" {
		req.Header.Set("Content-Type", rc.contentType)
	}
	// The guard wants an API content type or a non-empty X-Narad-Client
	// on anything that changes state. Sending it on every request costs
	// nothing and is what stops a body-less ack being refused with 415.
	req.Header.Set("X-Narad-Client", c.cfg.userAgent)
	req.Header.Set("User-Agent", c.cfg.userAgent)
	req.Header.Set("Accept", "application/json")
	if c.cfg.username != "" {
		req.SetBasicAuth(c.cfg.username, c.cfg.password)
	}

	start := time.Now()
	res, err := c.http.Do(req)
	elapsed := time.Since(start)

	if err != nil {
		connErr := &ConnError{
			Op: rc.op, Topic: rc.topic, Node: node.address,
			Err: err, reached: reachedServer(err),
		}
		c.emit(Event{
			Kind: EventRequest, Op: rc.op, Topic: rc.topic, Node: node.address,
			Attempt: attempt, Took: elapsed, Err: connErr,
		})
		return nil, connErr
	}
	defer res.Body.Close()

	if !accepted(rc.ok, res.StatusCode) {
		apiErr := c.readError(rc, node.address, res)
		c.emit(Event{
			Kind: EventRequest, Op: rc.op, Topic: rc.topic, Node: node.address,
			Attempt: attempt, Status: res.StatusCode, Took: elapsed, Err: apiErr,
		})
		return nil, apiErr
	}

	raw, err := io.ReadAll(res.Body)
	if err != nil {
		// The status arrived, so the server acted and only the body was
		// cut short. For anything that changes state, it happened.
		connErr := &ConnError{
			Op: rc.op, Topic: rc.topic, Node: node.address,
			Err: err, reached: true,
		}
		c.emit(Event{
			Kind: EventRequest, Op: rc.op, Topic: rc.topic, Node: node.address,
			Attempt: attempt, Status: res.StatusCode, Took: elapsed, Err: connErr,
		})
		return nil, connErr
	}
	c.emit(Event{
		Kind: EventRequest, Op: rc.op, Topic: rc.topic, Node: node.address,
		Attempt: attempt, Status: res.StatusCode, Took: elapsed,
	})
	return &reply{status: res.StatusCode, body: raw, header: res.Header, node: node.address}, nil
}

// readError turns a failed reply into an Error.
func (c *Client) readError(rc call, node string, res *http.Response) *Error {
	apiErr := &Error{
		Op: rc.op, Topic: rc.topic,
		Status:     res.StatusCode,
		Node:       node,
		RetryAfter: retryAfter(res.Header.Get("Retry-After")),
		kind:       statusError(res.StatusCode),
	}
	body, err := io.ReadAll(io.LimitReader(res.Body, maxErrorBody))
	if err != nil || len(body) == 0 {
		return apiErr
	}
	// Most errors are {"error":"..."}, but the cluster routing layer
	// answers some with plain text, so the body is kept either way.
	var wrapper struct {
		Error string `json:"error"`
	}
	if json.Unmarshal(body, &wrapper) == nil && wrapper.Error != "" {
		apiErr.Message = wrapper.Error
		return apiErr
	}
	apiErr.Message = strings.TrimSpace(string(body))
	return apiErr
}

// emit delivers an event, if anyone asked for them.
func (c *Client) emit(e Event) {
	if c.cfg.onEvent != nil {
		c.cfg.onEvent(e)
	}
}

// retryAfter reads the header in either of its forms, seconds or a date.
func retryAfter(value string) time.Duration {
	if value == "" {
		return 0
	}
	if seconds, err := strconv.Atoi(strings.TrimSpace(value)); err == nil {
		if seconds <= 0 {
			return 0
		}
		return time.Duration(seconds) * time.Second
	}
	if when, err := http.ParseTime(value); err == nil {
		if d := time.Until(when); d > 0 {
			return d
		}
	}
	return 0
}

// reachedServer reports whether a failed request got far enough that the
// server could have acted on it.
//
// What matters is whether anything was sent. A refused connection or a
// DNS failure never left; a timeout or a reset mid-flight may well have
// arrived, and for a produce that is the difference between a safe retry
// and a duplicate.
func reachedServer(err error) bool {
	if err == nil {
		return false
	}
	var dnsErr *net.DNSError
	if errors.As(err, &dnsErr) {
		return false
	}
	var opErr *net.OpError
	if errors.As(err, &opErr) && opErr.Op == "dial" {
		return false
	}
	return true
}

func accepted(statuses []int, got int) bool {
	for _, want := range statuses {
		if want == got {
			return true
		}
	}
	return false
}

// decode unmarshals a reply body, if there is one.
func decode(body []byte, v any, op string) error {
	if len(body) == 0 {
		return nil
	}
	if err := json.Unmarshal(body, v); err != nil {
		return fmt.Errorf("narad: %s: decode reply: %w", op, err)
	}
	return nil
}

// wait sleeps for d, or until the context ends. It reports whether the
// sleep finished.
func wait(ctx context.Context, d time.Duration) bool {
	if d <= 0 {
		return ctx.Err() == nil
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return true
	case <-ctx.Done():
		return false
	}
}

// Ping asks one node whether it is alive and ready to serve.
//
// The address must be one the client was created with. Probing a named
// node is the only useful shape for this: a probe sent through the
// client's own load balancing would answer for whichever node came next,
// which is the opposite of the question.
//
// It reports [ErrUnavailable] for a node that is up but not ready, which
// is the ordinary state during a rolling restart. The probe does not
// count against the node's circuit breaker, because polling readiness
// must not be the thing that takes a cluster out of rotation.
func (c *Client) Ping(ctx context.Context, address string) error {
	if c.closed.Load() {
		return ErrClosed
	}
	addresses, err := parseAddresses(address)
	if err != nil {
		return err
	}
	target := c.nodes.find(addresses[0])
	if target == nil {
		return fmt.Errorf("narad: ping: %w: %q is not one of this client's nodes", ErrBadRequest, address)
	}
	// Deliberately not through do(): no retrying onto other nodes, and
	// no observe(), so a node reporting itself unready does not open its
	// own breaker.
	_, err = c.attempt(ctx, target, call{
		method: http.MethodGet,
		path:   "/readyz",
		op:     "ping",
		ok:     []int{http.StatusOK},
	}, 0)
	return err
}
