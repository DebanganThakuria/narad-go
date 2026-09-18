package narad

import (
	"net/http"
	"time"
)

// An Option adjusts a [Client]. The defaults suit ordinary use, so most
// programs pass none.
type Option func(*config)

// config is the resolved client settings.
type config struct {
	username     string
	password     string
	timeout      time.Duration
	attempts     int
	backoff      backoff
	cautious     bool
	breakAfter   int
	breakFor     time.Duration
	maxIdleConns int
	userAgent    string
	onEvent      func(Event)
	log          Logger
	http         *http.Client
}

// defaults are chosen so that a client created with no options is the
// right one for an ordinary service.
func defaults() config {
	return config{
		timeout:  10 * time.Second,
		attempts: 4,
		backoff: backoff{
			base: 50 * time.Millisecond,
			max:  5 * time.Second,
		},
		breakAfter:   5,
		breakFor:     2 * time.Second,
		maxIdleConns: 64,
		userAgent:    userAgent,
	}
}

// WithAuth sets the credentials sent with every request. Narad requires
// them outside development mode.
func WithAuth(username, password string) Option {
	return func(c *config) {
		c.username, c.password = username, password
	}
}

// WithTimeout sets how long one attempt may take. It defaults to ten
// seconds.
//
// It deliberately does not bound a long-polling consume, whose deadline
// comes from its own wait. A client-wide timeout shorter than the poll
// would cancel every poll that was doing exactly what it was told to.
func WithTimeout(d time.Duration) Option {
	return func(c *config) { c.timeout = d }
}

// WithRetries sets how many attempts a request gets in total, including
// the first. It defaults to four. One disables retrying.
func WithRetries(attempts int) Option {
	return func(c *config) { c.attempts = attempts }
}

// WithBackoff sets the delay before the first retry and the ceiling it
// grows to. It defaults to 50ms and 5s.
//
// The delay is randomized across the whole interval, and that is not
// decoration. When a broker sheds load, every client talking to it
// retries; without the spread they all retry in the same millisecond and
// knock it over again just as it recovers.
func WithBackoff(first, max time.Duration) Option {
	return func(c *config) {
		c.backoff.base, c.backoff.max = first, max
	}
}

// WithCautiousRetries stops the client retrying when it cannot tell
// whether the server already applied the request.
//
// The trade cannot be avoided, only chosen. Retrying an uncertain
// produce may write the message twice; not retrying may drop it. The
// default retries, because consumers have to tolerate duplicates under
// at-least-once anyway and a lost message is not recoverable. Choose
// this when a duplicate is the worse outcome, and reconcile yourself:
// [Uncertain] reports exactly this case.
func WithCautiousRetries() Option {
	return func(c *config) { c.cautious = true }
}

// WithBreaker sets how many consecutive failures take a node out of
// rotation, and for how long. It defaults to five failures and two
// seconds.
//
// The breaker is per node on purpose. Narad keeps producing while any
// node lives, so one dead node should cost the requests already in
// flight to it and nothing else. A client-wide breaker would turn one
// bad node into a total outage.
func WithBreaker(failures int, cooldown time.Duration) Option {
	return func(c *config) {
		c.breakAfter, c.breakFor = failures, cooldown
	}
}

// WithoutBreaker sends to every node regardless of how it has been
// behaving. Useful against a single node, or behind a load balancer that
// is already doing this job.
func WithoutBreaker() Option {
	return func(c *config) { c.breakAfter = 0 }
}

// WithIdleConnections sets how many idle connections to keep per node.
// It defaults to 64.
//
// The default is far above Go's own, because consuming is a long poll: N
// workers hold N connections open for seconds at a time, and with Go's
// default of two, every worker past the second reconnects on each poll.
func WithIdleConnections(n int) Option {
	return func(c *config) { c.maxIdleConns = n }
}

// WithUserAgent overrides the User-Agent, and the X-Narad-Client header
// that satisfies the server's cross-site guard. An empty value is
// ignored, since the guard requires a non-empty one.
func WithUserAgent(name string) Option {
	return func(c *config) {
		if name != "" {
			c.userAgent = name
		}
	}
}

// WithHTTPClient supplies the HTTP client to use, for callers who need
// their own transport, proxy or TLS settings.
//
// Leave its Timeout at zero. It applies to every request including long
// polls, so a Timeout shorter than a consume's wait cancels polls that
// were behaving correctly.
func WithHTTPClient(hc *http.Client) Option {
	return func(c *config) { c.http = hc }
}

// WithEvents installs a callback for metrics and logging.
//
// It runs on the calling goroutine, so keep it cheap and do not call
// back into the client from it.
func WithEvents(fn func(Event)) Option {
	return func(c *config) { c.onEvent = fn }
}

// EventKind says what an [Event] is reporting.
type EventKind string

// The kinds of event a client reports.
const (
	// EventRequest is one completed HTTP attempt, successful or not.
	EventRequest EventKind = "request"
	// EventRetry is a retry about to be waited out.
	EventRetry EventKind = "retry"
	// EventNode is a node changing circuit-breaker state.
	EventNode EventKind = "node"
)

// An Event reports something the client did, for metrics and logging.
// Which fields are set depends on Kind.
type Event struct {
	Kind  EventKind
	Op    string
	Topic string
	Node  string

	// Attempt is the zero-based attempt number, for requests and
	// retries.
	Attempt int
	// Status is the HTTP status, for a request that got one.
	Status int
	// Took is how long a request took.
	Took time.Duration
	// Wait is how long a retry is about to sleep.
	Wait time.Duration
	// State is the node's new circuit-breaker state, for EventNode.
	State string
	// Err is the failure, nil on success.
	Err error
	// Uncertain reports whether the failure being retried may have been
	// applied by the server anyway.
	Uncertain bool
}
