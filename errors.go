package narad

import (
	"errors"
	"fmt"
	"net/http"
	"time"
)

// Sentinel errors. Match them with errors.Is rather than comparing
// status codes or reading messages.
var (
	// ErrBadRequest means the server rejected the request as malformed.
	// Sending it again unchanged will fail the same way.
	ErrBadRequest = errors.New("narad: bad request")

	// ErrUnauthenticated means the credentials were missing or wrong.
	ErrUnauthenticated = errors.New("narad: unauthenticated")

	// ErrForbidden means the credentials were valid but hold no grant
	// for this action on this topic.
	ErrForbidden = errors.New("narad: forbidden")

	// ErrNotFound means the topic, user or other named thing is not
	// there.
	ErrNotFound = errors.New("narad: not found")

	// ErrExists means the state you assumed is not the state that is
	// there. Creating a topic that already exists reports it, which is
	// why EnsureTopic treats it as success, and so does a conditional
	// schema update whose base version is no longer current.
	ErrExists = errors.New("narad: already exists")

	// ErrLeaseLost means the message's visibility window closed before
	// the ack arrived, or it was already acked and redelivered under a
	// new handle. The message is not lost; it is on its way to somebody,
	// possibly another consumer, so do not treat its work as committed.
	ErrLeaseLost = errors.New("narad: lease lost")

	// ErrNoLease means Ack, Nack or Extend was called on a message that
	// holds no lease. Only ReadAt returns such messages: a read at an
	// offset reserves nothing, so there is nothing to settle.
	ErrNoLease = errors.New("narad: message holds no lease")

	// ErrOffsetGone means a read asked for an offset that is no longer
	// readable, because it aged out of retention. Skip forward; it is
	// not coming back.
	ErrOffsetGone = errors.New("narad: offset gone")

	// ErrTooLarge means the message exceeded the server's size limit.
	// See MaxMessageBytes.
	ErrTooLarge = errors.New("narad: message too large")

	// ErrThrottled means the server is shedding load, or this identity
	// has too many consumes in flight. Back off, or use fewer workers.
	ErrThrottled = errors.New("narad: throttled")

	// ErrUnavailable means this node could not serve the request now,
	// usually because a partition's owner is down. Another node, or the
	// same one later, may do better.
	ErrUnavailable = errors.New("narad: unavailable")

	// ErrServer means the server failed internally.
	ErrServer = errors.New("narad: server error")

	// ErrNoNodes means every node is currently failing its circuit
	// breaker, so nothing was sent.
	ErrNoNodes = errors.New("narad: no healthy nodes")

	// ErrClosed means the client has been closed.
	ErrClosed = errors.New("narad: client closed")
)

// An Error is a reply the server sent that could not be treated as
// success.
type Error struct {
	// Op is the operation attempted, such as "produce" or "ack".
	Op string
	// Topic is the topic involved, empty when there was none.
	Topic string
	// Status is the HTTP status code.
	Status int
	// Message is the server's explanation.
	Message string
	// Node is the address that answered.
	Node string
	// RetryAfter is the delay the server asked for, zero when it asked
	// for none.
	RetryAfter time.Duration

	kind error
}

func (e *Error) Error() string {
	where := e.Op
	if e.Topic != "" {
		where += " " + e.Topic
	}
	if e.Message == "" {
		return fmt.Sprintf("narad: %s: %s", where, http.StatusText(e.Status))
	}
	return fmt.Sprintf("narad: %s: %s (%d)", where, e.Message, e.Status)
}

// Unwrap exposes the sentinel for this status, so errors.Is finds it.
func (e *Error) Unwrap() error { return e.kind }

// A ConnError is a request that never got a reply: the connection
// failed, the deadline passed, or the response was cut off.
type ConnError struct {
	// Op is the operation attempted.
	Op string
	// Topic is the topic involved, empty when there was none.
	Topic string
	// Node is the address the request went to.
	Node string
	// Err is the underlying cause.
	Err error

	// reached records whether the request got far enough that the server
	// could have acted on it.
	reached bool
}

func (e *ConnError) Error() string {
	where := e.Op
	if e.Topic != "" {
		where += " " + e.Topic
	}
	if e.reached {
		return fmt.Sprintf("narad: %s: %v (the server may have applied it)", where, e.Err)
	}
	return fmt.Sprintf("narad: %s: %v", where, e.Err)
}

// Unwrap exposes the underlying network or context error.
func (e *ConnError) Unwrap() error { return e.Err }

// Retryable reports whether sending the same request again could
// plausibly succeed.
//
// It says nothing about whether doing so is safe. A retryable error can
// also be uncertain, and for anything that changes state those two
// together mean a retry may duplicate. Ask [Uncertain] as well when that
// matters.
func Retryable(err error) bool {
	var apiErr *Error
	if errors.As(err, &apiErr) {
		switch apiErr.Status {
		case http.StatusTooManyRequests,
			http.StatusServiceUnavailable,
			http.StatusMisdirectedRequest,
			http.StatusRequestTimeout,
			http.StatusBadGateway,
			http.StatusGatewayTimeout,
			http.StatusInternalServerError:
			return true
		}
		return false
	}
	var connErr *ConnError
	if errors.As(err, &connErr) {
		return true
	}
	return errors.Is(err, ErrNoNodes)
}

// Uncertain reports whether the server may have carried out the request
// even though it returned an error.
//
// This is the question that decides whether retrying can duplicate, and
// the status alone cannot answer it. A 503 means opposite things
// depending on the operation: on ack and consume the router declined
// before anything could act, while a produce can get one from a control
// plane that went away partway through. Getting that backwards would
// make an ack that never happened look as though it did, which is the
// one outcome the lease exists to prevent.
func Uncertain(err error) bool {
	var connErr *ConnError
	if errors.As(err, &connErr) {
		return connErr.reached
	}
	var apiErr *Error
	if errors.As(err, &apiErr) {
		switch apiErr.Status {
		case http.StatusInternalServerError:
			// The server was already working on it.
			return true
		case http.StatusBadGateway:
			// The forward to the owner broke in flight, so the owner may
			// have acted on it.
			return true
		case http.StatusServiceUnavailable:
			return apiErr.Op == opProduce
		}
	}
	return false
}

// statusError maps a status to its sentinel.
func statusError(status int) error {
	switch status {
	case http.StatusBadRequest, http.StatusUnsupportedMediaType:
		return ErrBadRequest
	case http.StatusUnauthorized:
		return ErrUnauthenticated
	case http.StatusForbidden:
		return ErrForbidden
	case http.StatusNotFound:
		return ErrNotFound
	case http.StatusConflict:
		return ErrExists
	case http.StatusGone:
		return ErrLeaseLost
	case http.StatusRequestEntityTooLarge:
		return ErrTooLarge
	case http.StatusTooManyRequests:
		return ErrThrottled
	case http.StatusServiceUnavailable, http.StatusMisdirectedRequest:
		return ErrUnavailable
	}
	if status >= 500 {
		return ErrServer
	}
	return ErrBadRequest
}
