/*
Package narad is the Go client for the Narad message broker.

The front door is SQS and the inside is Kafka.

What you write against is a work queue. You take one message, hold it
under a visibility timeout, and ack it or let it come back. There are no
consumer groups, no partition assignment and no offsets to commit. Bring
an SQS mental model and almost everything here is where you expect it.

Underneath, a topic is a partitioned append-only log with offsets and
retention, the way Kafka is. That is what pays for the things a queue
normally cannot do: a message survives being consumed, replay is a read
at an offset rather than a redelivery, and fanning out to a child topic
is cheap because the log is already there.

# Getting started

Three calls cover almost everything:

	client, err := narad.New("localhost:7942")
	if err != nil {
		return err
	}
	defer client.Close()

	err = client.Produce(ctx, "orders", order)

	err = client.Consume(ctx, "orders", narad.HandlerFunc(
		func(ctx context.Context, msg *narad.Message) error {
			var order Order
			if err := msg.Into(&order); err != nil {
				return err
			}
			return process(ctx, order)
		}))

The defaults are meant to be right without thinking about them. Consume
long-polls, runs one worker, keeps each message's lease alive while its
handler runs, acks on success and hands the message back on failure.
Everything is adjustable with options, and none of it has to be.

Pass every node you have, comma separated, and the client spreads work
across them and routes around the ones that are failing:

	client, err := narad.New("n1:7942,n2:7942,n3:7942",
		narad.WithAuth("svc", os.Getenv("NARAD_PASSWORD")))

# At-least-once, and what it asks of you

Handlers must be idempotent. Narad delivers at least once and does not
deduplicate, so a broker restart can redeliver a message whose ack was
still in the batch being persisted. This is not something to plan around
later.

The same property shapes producing. When a produce fails, [Uncertain]
reports whether the broker may have applied it anyway. Those are retried
by default, because a duplicate is recoverable and a lost message is
not, and [WithCautiousRetries] hands that choice back to you.

# Errors

Errors answer the questions a caller acts on, so nobody has to match on
strings:

	narad.Retryable(err)              // worth trying again
	narad.Uncertain(err)              // might have happened anyway
	errors.Is(err, narad.ErrNotFound) // which assumption was wrong

[Error] carries the status, the server's message and the node that
answered.

# What this package will not do for you

It will not give you ordering, because Narad does not have it. It will
not deduplicate. It will not hide the lease: a handler that runs longer
than the visibility timeout without renewing loses its message, and
Consume renews it rather than pretending otherwise.
*/
package narad
