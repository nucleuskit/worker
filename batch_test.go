package worker

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/nucleuskit/nucleus/cap/mq"
)

func TestRunnerConsumeBatchFlushesAtMaxMessages(t *testing.T) {
	deliveries := []*testDelivery{newTestDelivery(), newTestDelivery()}
	consumer := bufferedConsumer(
		mqDelivery("msg-1", deliveries[0]),
		mqDelivery("msg-2", deliveries[1]),
	)
	var got []Message
	runner := NewRunner(BatchHandlerFunc(func(ctx context.Context, messages []Message) error {
		got = append(got, messages...)
		return nil
	}), WithBatchPolicy(BatchPolicy{MaxMessages: 2, FlushInterval: time.Hour}))

	if err := runner.Consume(context.Background(), consumer); err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Fatalf("expected one batch with 2 messages, got %d", len(got))
	}
	for i, delivery := range deliveries {
		if !delivery.acked || delivery.nacked {
			t.Fatalf("delivery %d expected ack only, got ack=%v nack=%v", i, delivery.acked, delivery.nacked)
		}
	}
}

func TestRunnerConsumeBatchFlushesAtInterval(t *testing.T) {
	delivery := newTestDelivery()
	deliveries := make(chan mq.Delivery, 1)
	deliveries <- mqDelivery("msg-1", delivery)
	consumer := mq.ConsumerFunc(func(ctx context.Context) (<-chan mq.Delivery, error) {
		return deliveries, nil
	})
	flushed := make(chan []Message, 1)
	runner := NewRunner(BatchHandlerFunc(func(ctx context.Context, messages []Message) error {
		flushed <- append([]Message(nil), messages...)
		return nil
	}), WithBatchPolicy(BatchPolicy{MaxMessages: 10, FlushInterval: 10 * time.Millisecond}))

	errCh := make(chan error, 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		errCh <- runner.Consume(ctx, consumer)
	}()

	select {
	case got := <-flushed:
		if len(got) != 1 || got[0].ID != "msg-1" {
			t.Fatalf("unexpected flushed batch: %#v", got)
		}
	case <-time.After(200 * time.Millisecond):
		t.Fatal("timed out waiting for interval flush")
	}
	cancel()
	if err := <-errCh; !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context canceled, got %v", err)
	}
	if !delivery.acked || delivery.nacked {
		t.Fatalf("expected delivery acked after interval flush, got ack=%v nack=%v", delivery.acked, delivery.nacked)
	}
}

func TestRunnerConsumeBatchNacksAllDeliveriesOnHandlerFailure(t *testing.T) {
	handlerErr := errors.New("batch failed")
	deliveries := []*testDelivery{newTestDelivery(), newTestDelivery()}
	consumer := bufferedConsumer(
		mqDelivery("msg-1", deliveries[0]),
		mqDelivery("msg-2", deliveries[1]),
	)
	runner := NewRunner(BatchHandlerFunc(func(ctx context.Context, messages []Message) error {
		return handlerErr
	}), WithBatchPolicy(BatchPolicy{MaxMessages: 2, FlushInterval: time.Hour}))

	err := runner.Consume(context.Background(), consumer)

	if !errors.Is(err, handlerErr) {
		t.Fatalf("expected handler error, got %v", err)
	}
	for i, delivery := range deliveries {
		if delivery.acked || !delivery.nacked {
			t.Fatalf("delivery %d expected nack only, got ack=%v nack=%v", i, delivery.acked, delivery.nacked)
		}
		if !errors.Is(delivery.nackCause, handlerErr) {
			t.Fatalf("delivery %d expected nack cause %v, got %v", i, handlerErr, delivery.nackCause)
		}
	}
}

func TestRunnerConsumeBatchAppliesExplicitPerMessageDecision(t *testing.T) {
	nackErr := errors.New("reject message")
	deliveries := []*testDelivery{newTestDelivery(), newTestDelivery(), newTestDelivery()}
	consumer := bufferedConsumer(
		mqDelivery("msg-1", deliveries[0]),
		mqDelivery("msg-2", deliveries[1]),
		mqDelivery("msg-3", deliveries[2]),
	)
	runner := NewRunner(batchResultHandlerFunc(func(ctx context.Context, messages []Message) (BatchResult, error) {
		return BatchResult{Decisions: []BatchDecision{
			{Index: 1, Action: BatchActionNack, Cause: nackErr},
		}}, nil
	}), WithBatchPolicy(BatchPolicy{MaxMessages: 3, FlushInterval: time.Hour}))

	if err := runner.Consume(context.Background(), consumer); err != nil {
		t.Fatal(err)
	}
	if !deliveries[0].acked || deliveries[0].nacked {
		t.Fatalf("first delivery expected ack only, got ack=%v nack=%v", deliveries[0].acked, deliveries[0].nacked)
	}
	if deliveries[1].acked || !deliveries[1].nacked {
		t.Fatalf("second delivery expected nack only, got ack=%v nack=%v", deliveries[1].acked, deliveries[1].nacked)
	}
	if !errors.Is(deliveries[1].nackCause, nackErr) {
		t.Fatalf("second delivery expected nack cause %v, got %v", nackErr, deliveries[1].nackCause)
	}
	if !deliveries[2].acked || deliveries[2].nacked {
		t.Fatalf("third delivery expected ack only, got ack=%v nack=%v", deliveries[2].acked, deliveries[2].nacked)
	}
}

func TestRunnerConsumeBatchAppliesRetryAndDeadLetterDecisions(t *testing.T) {
	retryErr := errors.New("retry message")
	deadErr := errors.New("dead letter message")
	deliveries := []*testDelivery{newTestDelivery(), newTestDelivery(), newTestDelivery()}
	consumer := bufferedConsumer(
		mqDelivery("msg-1", deliveries[0]),
		mqDelivery("msg-2", deliveries[1]),
		mqDelivery("msg-3", deliveries[2]),
	)
	runner := NewRunner(batchResultHandlerFunc(func(ctx context.Context, messages []Message) (BatchResult, error) {
		return BatchResult{Decisions: []BatchDecision{
			{Index: 0, Action: BatchActionAck},
			{Index: 1, Action: BatchActionRetry, Cause: retryErr, RetryAfter: time.Millisecond},
			{Index: 2, Action: BatchActionDeadLetter, Cause: deadErr},
		}}, nil
	}), WithBatchPolicy(BatchPolicy{MaxMessages: 3, FlushInterval: time.Hour}), WithDeadLetterPolicy(mq.DeadLetterPolicy{Topic: "jobs.dlq"}))

	if err := runner.Consume(context.Background(), consumer); err != nil {
		t.Fatal(err)
	}
	if !deliveries[0].acked || deliveries[0].nacked {
		t.Fatalf("first delivery expected ack only, got ack=%v nack=%v", deliveries[0].acked, deliveries[0].nacked)
	}
	if len(deliveries[1].decisions) == 0 || deliveries[1].decisions[len(deliveries[1].decisions)-1].Action != mq.DecisionRetry {
		t.Fatalf("second delivery expected retry decision, got %#v", deliveries[1].decisions)
	}
	if len(deliveries[2].decisions) == 0 || deliveries[2].decisions[len(deliveries[2].decisions)-1].Action != mq.DecisionDeadLetter {
		t.Fatalf("third delivery expected dead-letter decision, got %#v", deliveries[2].decisions)
	}
	if deliveries[2].decisions[len(deliveries[2].decisions)-1].DeadLetter.Topic != "jobs.dlq" {
		t.Fatalf("unexpected dead-letter metadata: %#v", deliveries[2].decisions)
	}
}

func TestRunnerConsumeBatchRetriesHandlerFailure(t *testing.T) {
	handlerErr := errors.New("batch failed")
	attempts := 0
	deliveries := []*testDelivery{newTestDelivery(), newTestDelivery()}
	consumer := bufferedConsumer(
		mqDelivery("msg-1", deliveries[0]),
		mqDelivery("msg-2", deliveries[1]),
	)
	runner := NewRunner(BatchHandlerFunc(func(ctx context.Context, messages []Message) error {
		attempts++
		if attempts == 1 {
			return handlerErr
		}
		return nil
	}), WithBatchPolicy(BatchPolicy{MaxMessages: 2, FlushInterval: time.Hour}), WithRetryPolicy(mq.RetryPolicy{MaxAttempts: 2}))

	if err := runner.Consume(context.Background(), consumer); err != nil {
		t.Fatal(err)
	}
	if attempts != 2 {
		t.Fatalf("expected batch retry, got %d attempts", attempts)
	}
	for i, delivery := range deliveries {
		if !delivery.acked || delivery.nacked {
			t.Fatalf("delivery %d expected ack only after retry, got ack=%v nack=%v", i, delivery.acked, delivery.nacked)
		}
	}
}

func TestRunnerConsumeBatchRecoversPanic(t *testing.T) {
	deliveries := []*testDelivery{newTestDelivery(), newTestDelivery()}
	consumer := bufferedConsumer(
		mqDelivery("msg-1", deliveries[0]),
		mqDelivery("msg-2", deliveries[1]),
	)
	runner := NewRunner(BatchHandlerFunc(func(ctx context.Context, messages []Message) error {
		panic("boom")
	}), WithBatchPolicy(BatchPolicy{MaxMessages: 2, FlushInterval: time.Hour}))

	err := runner.Consume(context.Background(), consumer)

	if !errors.Is(err, ErrPanicRecovered) {
		t.Fatalf("expected ErrPanicRecovered, got %v", err)
	}
	for i, delivery := range deliveries {
		if delivery.acked || !delivery.nacked {
			t.Fatalf("delivery %d expected nack after panic, got ack=%v nack=%v", i, delivery.acked, delivery.nacked)
		}
	}
}

func TestRunnerConsumeBatchInvalidDecisionNacksAllDeliveries(t *testing.T) {
	deliveries := []*testDelivery{newTestDelivery(), newTestDelivery()}
	consumer := bufferedConsumer(
		mqDelivery("msg-1", deliveries[0]),
		mqDelivery("msg-2", deliveries[1]),
	)
	runner := NewRunner(batchResultHandlerFunc(func(ctx context.Context, messages []Message) (BatchResult, error) {
		return BatchResult{Decisions: []BatchDecision{{Index: 9, Action: BatchActionAck}}}, nil
	}), WithBatchPolicy(BatchPolicy{MaxMessages: 2, FlushInterval: time.Hour}))

	err := runner.Consume(context.Background(), consumer)

	if err == nil {
		t.Fatal("expected invalid decision error")
	}
	for i, delivery := range deliveries {
		if delivery.acked || !delivery.nacked {
			t.Fatalf("delivery %d expected nack only after invalid decision, got ack=%v nack=%v", i, delivery.acked, delivery.nacked)
		}
	}
}

func TestRunnerConsumeBatchDrainsPendingDeliveriesAfterContextCancel(t *testing.T) {
	deliveries := []*testDelivery{newTestDelivery(), newTestDelivery()}
	source := make(chan mq.Delivery, 2)
	source <- mqDelivery("msg-1", deliveries[0])
	source <- mqDelivery("msg-2", deliveries[1])
	close(source)
	ctx, cancel := context.WithCancel(context.Background())
	consumer := mq.ConsumerFunc(func(context.Context) (<-chan mq.Delivery, error) {
		cancel()
		return source, nil
	})
	var got []Message
	runner := NewRunner(BatchHandlerFunc(func(ctx context.Context, messages []Message) error {
		if err := ctx.Err(); err != nil {
			t.Fatalf("drain handler should not receive canceled context: %v", err)
		}
		got = append(got, messages...)
		return nil
	}), WithBatchPolicy(BatchPolicy{MaxMessages: 10, FlushInterval: time.Hour}))

	err := runner.Consume(ctx, consumer)

	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context canceled after drain, got %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("expected pending deliveries drained, got %d", len(got))
	}
	for i, delivery := range deliveries {
		if !delivery.acked || delivery.nacked {
			t.Fatalf("delivery %d expected ack only after drain, got ack=%v nack=%v", i, delivery.acked, delivery.nacked)
		}
	}
}

func bufferedConsumer(deliveries ...mq.Delivery) mq.Consumer {
	return mq.ConsumerFunc(func(ctx context.Context) (<-chan mq.Delivery, error) {
		ch := make(chan mq.Delivery, len(deliveries))
		for _, delivery := range deliveries {
			ch <- delivery
		}
		close(ch)
		return ch, nil
	})
}

func mqDelivery(id string, delivery *testDelivery) mq.Delivery {
	return mq.Delivery{
		Message: mq.Message{ID: id, Topic: "jobs"},
		Ack:     delivery.Ack,
		Nack:    delivery.Nack,
		Decide:  delivery.Decide,
	}
}

type batchResultHandlerFunc func(context.Context, []Message) (BatchResult, error)

func (fn batchResultHandlerFunc) Handle(ctx context.Context, message Message) error {
	result, err := fn(ctx, []Message{message})
	if applyErr := applyBatchResult(ctx, []batchDelivery{{message: message}}, result, err); applyErr != nil {
		return applyErr
	}
	return err
}

func (fn batchResultHandlerFunc) HandleBatchResult(ctx context.Context, messages []Message) (BatchResult, error) {
	return fn(ctx, messages)
}
