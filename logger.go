package narad

// A Logger receives the client's diagnostic messages.
//
// The method set is *slog.Logger's, so the standard library satisfies it
// with no adapter:
//
//	client, err := narad.New(addr, narad.WithLogger(slog.Default()))
//
// Any other logger needs a handful of forwarding methods. Arguments come
// as alternating keys and values, the way slog takes them.
//
// The client logs sparingly, because a library that fills someone else's
// logs is a library they turn off: retries and node state changes at
// warn, and the failures a consumer cannot report any other way at
// error. For anything finer grained, use [WithEvents].
type Logger interface {
	Debug(msg string, args ...any)
	Info(msg string, args ...any)
	Warn(msg string, args ...any)
	Error(msg string, args ...any)
}

// WithLogger sends the client's diagnostics to a logger. Without one it
// stays silent.
func WithLogger(l Logger) Option {
	return func(c *config) { c.log = l }
}

// nopLogger discards everything, so the call sites need no nil checks.
type nopLogger struct{}

func (nopLogger) Debug(string, ...any) {}
func (nopLogger) Info(string, ...any)  {}
func (nopLogger) Warn(string, ...any)  {}
func (nopLogger) Error(string, ...any) {}

// logger returns the configured logger, or one that discards, so the
// call sites need no nil checks.
func (c *Client) logger() Logger {
	if c.cfg.log == nil {
		return nopLogger{}
	}
	return c.cfg.log
}
