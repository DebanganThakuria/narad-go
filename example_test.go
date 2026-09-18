package narad_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	narad "github.com/debanganthakuria/narad-go"
)

type Order struct {
	ID     string `json:"id"`
	Amount int64  `json:"amount"`
}

// The whole of a working producer.
func Example() {
	client, err := narad.New("localhost:7942")
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	err = client.Produce(context.Background(), "orders", Order{ID: "ord_1", Amount: 4999})
	if err != nil {
		log.Fatal(err)
	}
}

// In production, give it every node and its credentials. Nothing else
// needs configuring.
func ExampleNew() {
	client, err := narad.New("n1:7942,n2:7942,n3:7942",
		narad.WithAuth("svc-orders", os.Getenv("NARAD_PASSWORD")),
		narad.WithLogger(slog.Default()),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()
}

// Consume is the call most programs want: it long-polls, runs a worker
// pool, keeps each message's lease alive while its handler runs, and
// settles it afterwards.
func ExampleClient_Consume() {
	client, _ := narad.New("localhost:7942")
	defer client.Close()

	// Ctrl-C drains: in-flight handlers finish and ack before Consume
	// returns, so their work is not repeated on the next start.
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	err := client.Consume(ctx, "orders", narad.HandlerFunc(
		func(ctx context.Context, msg *narad.Message) error {
			var order Order
			if err := msg.Into(&order); err != nil {
				// A message that will never decode is poison. Returning
				// an error would redeliver it forever, so swallow it and
				// record it somewhere a person will look.
				slog.Error("undecodable message, dropping", "id", msg.ID(), "err", err)
				return nil
			}
			return process(ctx, order)
		}),
		narad.WithWorkers(8),
	)
	if err != nil {
		log.Fatal(err)
	}
}

// Slow work keeps its message: the lease is renewed underneath the
// handler, and the handler's context is cancelled if it is ever lost.
func ExampleClient_Consume_slowWork() {
	client, _ := narad.New("localhost:7942")
	defer client.Close()

	_ = client.Consume(context.Background(), "transcodes", narad.HandlerFunc(
		func(ctx context.Context, msg *narad.Message) error {
			for range 100 {
				// Checking the context is what makes losing the lease
				// safe: once cancelled, somebody else may have this
				// message.
				if ctx.Err() != nil {
					return ctx.Err()
				}
				time.Sleep(time.Second)
			}
			return nil
		}))
}

// Taking one message and settling it yourself, when a loop is not what
// you want.
func ExampleClient_Receive() {
	client, _ := narad.New("localhost:7942")
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	msg, err := client.Receive(ctx, "orders")
	if err != nil {
		log.Fatal(err)
	}

	var order Order
	if err := msg.Into(&order); err != nil {
		log.Fatal(err)
	}
	if err := process(ctx, order); err != nil {
		// Hand it back at once rather than making the next consumer wait
		// out the visibility timeout.
		_ = msg.Nack(ctx)
		return
	}
	if err := msg.Ack(ctx); err != nil && errors.Is(err, narad.ErrLeaseLost) {
		// Too slow: the message went back and somebody else will do it.
		// Nothing to undo, since handlers are idempotent.
		return
	}
}

// An envelope gives the message an id, a timestamp and somewhere to
// record attempts.
func ExampleWithEnvelope() {
	client, _ := narad.New("localhost:7942")
	defer client.Close()

	err := client.Produce(context.Background(), "orders", Order{ID: "ord_1"},
		narad.WithEnvelope(),
		narad.WithHeaders(map[string]string{"trace": "abc123"}),
	)
	if err != nil {
		log.Fatal(err)
	}
}

// Retry is what advances the attempt count, because the broker never
// rewrites a stored payload. Pair it with a delay child to back off.
func ExampleClient_Retry() {
	client, _ := narad.New("localhost:7942")
	defer client.Close()

	_ = client.Consume(context.Background(), "orders", narad.HandlerFunc(
		func(ctx context.Context, msg *narad.Message) error {
			var order Order
			if err := msg.Into(&order); err != nil {
				return err
			}
			err := process(ctx, order)
			if err == nil {
				return nil
			}

			env, envErr := msg.Envelope()
			if envErr != nil {
				return err // no envelope, so let the ordinary redelivery handle it
			}
			if env.Attempts >= 5 {
				// Out of road. Park it where a person can look, and ack
				// the original so it stops coming back.
				return client.Retry(ctx, msg, err, "orders-parked")
			}
			return client.Retry(ctx, msg, err, "orders-retry-30s")
		}))
}

// A topic can enforce a schema and still carry envelopes: the SDK wraps
// the schema so the broker validates the envelope and the message inside
// it.
func ExampleWithEnvelopeSchema() {
	client, _ := narad.New("localhost:7942")
	defer client.Close()

	schema := json.RawMessage(`{
		"type": "object",
		"properties": {
			"id":     {"type": "string"},
			"amount": {"type": "integer", "minimum": 0}
		},
		"required": ["id", "amount"],
		"additionalProperties": false
	}`)

	_, err := client.EnsureTopic(context.Background(), "orders",
		narad.WithPartitionCount(6),
		narad.WithRetention(24*time.Hour),
		narad.WithEnvelopeSchema(schema),
	)
	if err != nil {
		log.Fatal(err)
	}
	// Producers must now use an envelope, and a message that does not
	// match the schema is rejected before it reaches the log.
}

// Replay reads history without taking it off the queue.
func ExampleClient_ReadAt() {
	client, _ := narad.New("localhost:7942")
	defer client.Close()

	for offset := int64(0); ; offset++ {
		msg, err := client.ReadAt(context.Background(), "orders", 0, offset)
		if errors.Is(err, narad.ErrOffsetGone) {
			continue // aged out of retention, skip forward
		}
		if err != nil {
			log.Fatal(err)
		}
		if msg == nil {
			break // caught up with the end of the log
		}
		fmt.Println(msg.Offset, msg.Key)
	}
}

// Turning off uncertain retries is right when a duplicate is worse than
// a loss. You then reconcile yourself.
func ExampleWithCautiousRetries() {
	client, _ := narad.New("localhost:7942", narad.WithCautiousRetries())
	defer client.Close()

	err := client.Produce(context.Background(), "payments", Order{ID: "pay_1"})
	if narad.Uncertain(err) {
		log.Printf("unknown outcome, check before sending again: %v", err)
	}
}

func process(ctx context.Context, order Order) error { return nil }
