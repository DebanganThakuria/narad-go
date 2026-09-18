package narad

import (
	"errors"
	"math"
	"math/rand/v2"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

// backoff computes how long to wait before a retry.
type backoff struct {
	base time.Duration
	max  time.Duration
}

// delay returns the wait before the given retry, counting from zero.
//
// The result is spread uniformly over the interval rather than being the
// interval itself. That spread is the point: a broker that starts
// refusing requests is talking to many clients at once, and without it
// they all come back in the same millisecond.
func (b backoff) delay(attempt int) time.Duration {
	base, max := b.base, b.max
	if base <= 0 {
		base = 50 * time.Millisecond
	}
	if max <= 0 {
		max = 5 * time.Second
	}
	if attempt < 0 {
		attempt = 0
	}
	// Clamp before converting: math.Pow reaches +Inf long before an
	// attempt count gets silly, and converting that to a Duration is
	// undefined.
	scaled := float64(base) * math.Pow(2, float64(attempt))
	if math.IsInf(scaled, 1) || scaled > float64(max) {
		scaled = float64(max)
	}
	return time.Duration(rand.Float64() * scaled)
}

// breakerState is where a node's circuit breaker sits.
type breakerState int

const (
	// closed is healthy: requests flow.
	closed breakerState = iota
	// open means the node is presumed down and requests fail at once.
	open
	// halfOpen admits one request to find out whether it recovered.
	halfOpen
)

func (s breakerState) String() string {
	switch s {
	case closed:
		return "closed"
	case open:
		return "open"
	case halfOpen:
		return "half-open"
	}
	return "unknown"
}

// A node is one broker address and what the client knows about it.
type node struct {
	address string

	failAfter int
	openFor   time.Duration
	onChange  func(string, string)

	mu        sync.Mutex
	state     breakerState
	failures  int
	openedAt  time.Time
	lastError error
	now       func() time.Time
}

func newNode(address string, cfg config, onChange func(string, string)) *node {
	return &node{
		address:   address,
		failAfter: cfg.breakAfter,
		openFor:   cfg.breakFor,
		onChange:  onChange,
		now:       time.Now,
	}
}

// ready reports whether the node will take a request, moving it to
// half-open when its cooldown has passed.
func (n *node) ready() bool {
	if n.failAfter <= 0 {
		return true
	}
	n.mu.Lock()
	defer n.mu.Unlock()

	switch n.state {
	case closed:
		return true
	case open:
		if n.now().Sub(n.openedAt) < n.openFor {
			return false
		}
		n.setState(halfOpen)
		return true
	default:
		// Half-open admits exactly one request at a time: the point is
		// to ask a question, not to resume traffic before it is
		// answered.
		return false
	}
}

// observe records how a request to this node turned out.
func (n *node) observe(err error) {
	if n.failAfter <= 0 {
		return
	}
	if err == nil || !nodeFault(err) {
		n.succeed()
		return
	}
	n.fail(err)
}

func (n *node) succeed() {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.failures = 0
	n.lastError = nil
	n.setState(closed)
}

func (n *node) fail(err error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.lastError = err
	if n.state == halfOpen {
		// The probe answered the question, and the answer was no.
		n.openedAt = n.now()
		n.setState(open)
		return
	}
	n.failures++
	if n.failures >= n.failAfter {
		n.openedAt = n.now()
		n.setState(open)
	}
}

// setState moves the breaker. The caller holds the lock.
func (n *node) setState(next breakerState) {
	if n.state == next {
		return
	}
	n.state = next
	if next == closed {
		n.failures = 0
	}
	if n.onChange != nil {
		n.onChange(n.address, next.String())
	}
}

func (n *node) snapshot() NodeHealth {
	n.mu.Lock()
	defer n.mu.Unlock()
	return NodeHealth{
		Address:   n.address,
		Healthy:   n.state == closed,
		State:     n.state.String(),
		LastError: n.lastError,
	}
}

// nodeFault reports whether an error says anything about the node's
// health.
//
// Getting this wrong costs in both directions. Counting a 404 would let
// one caller asking for a missing topic take every node out of rotation.
// Not counting a 503 would leave a client hammering a node whose
// partitions are all unavailable. A 429 is excluded because Narad's is a
// per-identity cap on consumes in flight: every node answers the same
// way, so the fix is fewer workers, not a different node.
func nodeFault(err error) bool {
	if err == nil {
		return false
	}
	var connErr *ConnError
	if errors.As(err, &connErr) {
		return true
	}
	var apiErr *Error
	if errors.As(err, &apiErr) {
		switch apiErr.Status {
		case http.StatusInternalServerError,
			http.StatusBadGateway,
			http.StatusServiceUnavailable,
			http.StatusGatewayTimeout:
			return true
		}
	}
	return false
}

// A nodeSet holds the cluster's nodes and picks one per attempt.
//
// Selection is round-robin over the nodes whose breaker is letting
// traffic through, and round-robin is right here rather than anything
// cleverer: a produce is accepted by any live node, and cross-node
// consume means any node can serve any partition, so no node is the
// correct one for a given request. What matters is leaving out a dead
// one, which the breaker decides.
type nodeSet struct {
	all  []*node
	next atomic.Uint64
}

func newNodeSet(addresses []string, cfg config) *nodeSet {
	onChange := func(address, state string) {
		if cfg.onEvent != nil {
			cfg.onEvent(Event{Kind: EventNode, Node: address, State: state})
		}
		if cfg.log != nil {
			cfg.log.Warn("narad: node changed state", "node", address, "state", state)
		}
	}
	all := make([]*node, 0, len(addresses))
	for _, address := range addresses {
		all = append(all, newNode(address, cfg, onChange))
	}
	return &nodeSet{all: all}
}

// pick returns the next node that will take a request, preferring one
// this request has not already tried.
//
// Once every node has been tried it falls back to any that is ready, so
// a single-node client still gets its retries rather than a hard
// failure. It returns nil only when every breaker is open, which the
// caller turns into ErrNoNodes rather than hammering a cluster that is
// asking it to stop.
func (s *nodeSet) pick(tried map[string]bool) *node {
	count := len(s.all)
	if count == 0 {
		return nil
	}
	start := int(s.next.Add(1) - 1)
	for i := range count {
		candidate := s.all[(start+i)%count]
		if !tried[candidate.address] && candidate.ready() {
			return candidate
		}
	}
	for i := range count {
		if candidate := s.all[(start+i)%count]; candidate.ready() {
			return candidate
		}
	}
	return nil
}

// find returns the node with this address, or nil.
func (s *nodeSet) find(address string) *node {
	for _, candidate := range s.all {
		if candidate.address == address {
			return candidate
		}
	}
	return nil
}

func (s *nodeSet) health() []NodeHealth {
	out := make([]NodeHealth, 0, len(s.all))
	for _, candidate := range s.all {
		out = append(out, candidate.snapshot())
	}
	return out
}
