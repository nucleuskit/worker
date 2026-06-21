package worker

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/nucleuskit/nucleus/cap/mq"
)

func TestRunnerDispatchesMessage(t *testing.T) {
	var got Message
	runner := NewRunner(HandlerFunc(func(ctx context.Context, message Message) error {
		got = message
		return nil
	}))

	err := runner.Dispatch(context.Background(), Message{ID: "1", Topic: "hello"})
	if err != nil {
		t.Fatal(err)
	}
	if got.ID != "1" || got.Topic != "hello" {
		t.Fatalf("unexpected message: %#v", got)
	}
}

func TestRunnerRejectsAfterClose(t *testing.T) {
	runner := NewRunner(nil)
	if err := runner.Close(); err != nil {
		t.Fatal(err)
	}
	err := runner.Dispatch(context.Background(), Message{})
	if !errors.Is(err, ErrRunnerClosed) {
		t.Fatalf("expected ErrRunnerClosed, got %v", err)
	}
}

func TestRunnerHandlesMQDeliveryWithEnvelopeAndAck(t *testing.T) {
	var got Message
	runner := NewRunner(HandlerFunc(func(ctx context.Context, message Message) error {
		got = message
		return nil
	}))
	delivery := newTestDelivery()
	message := mq.Message{
		ID:      "msg-1",
		Topic:   "orders.created",
		Key:     "order-1",
		Body:    []byte(`{"id":"order-1"}`),
		Headers: map[string]string{"traceparent": "00-abc"},
		Metadata: mq.Metadata{
			Partition:       2,
			Offset:          42,
			DeliveryAttempt: 3,
			ReceivedAt:      time.Unix(100, 0),
		},
	}
	mqDelivery := mq.Delivery{Message: message, Ack: delivery.Ack, Nack: delivery.Nack}

	if err := runner.HandleDelivery(context.Background(), mqDelivery); err != nil {
		t.Fatal(err)
	}

	if !delivery.acked || delivery.nacked {
		t.Fatalf("expected ack only, got ack=%v nack=%v", delivery.acked, delivery.nacked)
	}
	if got.ID != "msg-1" || got.Topic != "orders.created" || got.Key != "order-1" {
		t.Fatalf("unexpected envelope identity: %#v", got)
	}
	if string(got.Payload) != `{"id":"order-1"}` {
		t.Fatalf("unexpected payload: %s", got.Payload)
	}
	if got.Headers["traceparent"] != "00-abc" {
		t.Fatalf("unexpected headers: %#v", got.Headers)
	}
	if got.Metadata.Partition != 2 || got.Metadata.Offset != 42 || got.Metadata.DeliveryAttempt != 3 {
		t.Fatalf("unexpected metadata: %#v", got.Metadata)
	}
}

func TestRunnerNacksFailedMQDelivery(t *testing.T) {
	handlerErr := errors.New("handler failed")
	runner := NewRunner(HandlerFunc(func(ctx context.Context, message Message) error {
		return handlerErr
	}))
	delivery := newTestDelivery()
	message := mq.Message{ID: "msg-1", Topic: "orders.created"}
	mqDelivery := mq.Delivery{Message: message, Ack: delivery.Ack, Nack: delivery.Nack}

	err := runner.HandleDelivery(context.Background(), mqDelivery)

	if !errors.Is(err, handlerErr) {
		t.Fatalf("expected handler error, got %v", err)
	}
	if delivery.acked || !delivery.nacked {
		t.Fatalf("expected nack only, got ack=%v nack=%v", delivery.acked, delivery.nacked)
	}
	if !errors.Is(delivery.nackCause, handlerErr) {
		t.Fatalf("expected nack cause %v, got %v", handlerErr, delivery.nackCause)
	}
}

func TestRunnerRetriesMQDeliveryBeforeAck(t *testing.T) {
	handlerErr := errors.New("try again")
	attempts := 0
	var events []Event
	runner := NewRunner(HandlerFunc(func(ctx context.Context, message Message) error {
		attempts++
		if attempts < 3 {
			return handlerErr
		}
		return nil
	}), WithRetryPolicy(mq.RetryPolicy{MaxAttempts: 3}), WithHook(HookFunc(func(ctx context.Context, event Event) {
		events = append(events, event)
	})))
	delivery := newTestDelivery()
	mqDelivery := mq.Delivery{
		Message: mq.Message{ID: "msg-1", Topic: "jobs", Metadata: mq.Metadata{DeliveryAttempt: 1}},
		Ack:     delivery.Ack,
		Nack:    delivery.Nack,
		Decide:  delivery.Decide,
	}

	if err := runner.HandleDelivery(context.Background(), mqDelivery); err != nil {
		t.Fatal(err)
	}
	if attempts != 3 {
		t.Fatalf("expected 3 attempts, got %d", attempts)
	}
	if !delivery.acked || delivery.nacked {
		t.Fatalf("expected ack after retry, got ack=%v nack=%v", delivery.acked, delivery.nacked)
	}
	retryEvents := 0
	for _, event := range events {
		if event.Kind == EventMessageRetried {
			retryEvents++
		}
	}
	if retryEvents != 2 {
		t.Fatalf("expected 2 retry events, got %d from %#v", retryEvents, events)
	}
}

func TestRunnerDeadLettersAfterRetryExhausted(t *testing.T) {
	handlerErr := errors.New("handler failed")
	runner := NewRunner(HandlerFunc(func(ctx context.Context, message Message) error {
		return handlerErr
	}), WithRetryPolicy(mq.RetryPolicy{MaxAttempts: 2}), WithDeadLetterPolicy(mq.DeadLetterPolicy{Topic: "jobs.dlq"}))
	delivery := newTestDelivery()
	mqDelivery := mq.Delivery{
		Message: mq.Message{ID: "msg-1", Topic: "jobs", Metadata: mq.Metadata{Group: "workers", DeliveryAttempt: 1}},
		Ack:     delivery.Ack,
		Nack:    delivery.Nack,
		Decide:  delivery.Decide,
	}

	err := runner.HandleDelivery(context.Background(), mqDelivery)

	if !errors.Is(err, handlerErr) {
		t.Fatalf("expected handler error, got %v", err)
	}
	if len(delivery.decisions) == 0 {
		t.Fatal("expected delivery decision")
	}
	decision := delivery.decisions[len(delivery.decisions)-1]
	if decision.Action != mq.DecisionDeadLetter {
		t.Fatalf("expected dead-letter decision, got %#v", decision)
	}
	if decision.DeadLetter.Topic != "jobs.dlq" || decision.DeadLetter.OriginalGroup != "workers" || decision.DeadLetter.Attempts != 2 {
		t.Fatalf("unexpected dead-letter metadata: %#v", decision.DeadLetter)
	}
}

func TestRunnerRecoversPanicAndNacksDelivery(t *testing.T) {
	runner := NewRunner(HandlerFunc(func(ctx context.Context, message Message) error {
		panic("boom")
	}))
	delivery := newTestDelivery()
	mqDelivery := mq.Delivery{Message: mq.Message{ID: "msg-1", Topic: "jobs"}, Ack: delivery.Ack, Nack: delivery.Nack}

	err := runner.HandleDelivery(context.Background(), mqDelivery)

	if !errors.Is(err, ErrPanicRecovered) {
		t.Fatalf("expected ErrPanicRecovered, got %v", err)
	}
	if delivery.acked || !delivery.nacked {
		t.Fatalf("expected panic delivery nacked, got ack=%v nack=%v", delivery.acked, delivery.nacked)
	}
}

func TestRunnerAppliesExplicitDeliveryDecision(t *testing.T) {
	rejectErr := errors.New("reject")
	runner := NewRunner(DecisionHandlerFunc(func(ctx context.Context, message Message) (DeliveryDecision, error) {
		return DeliveryDecision{Action: DeliveryActionNack, Cause: rejectErr}, nil
	}))
	delivery := newTestDelivery()
	mqDelivery := mq.Delivery{Message: mq.Message{ID: "msg-1", Topic: "jobs"}, Ack: delivery.Ack, Nack: delivery.Nack, Decide: delivery.Decide}

	err := runner.HandleDelivery(context.Background(), mqDelivery)

	if !errors.Is(err, rejectErr) {
		t.Fatalf("expected explicit reject error, got %v", err)
	}
	if len(delivery.decisions) != 1 || delivery.decisions[0].Action != mq.DecisionNack {
		t.Fatalf("expected nack decision, got %#v", delivery.decisions)
	}
}

func TestRunnerConsumesMQConsumer(t *testing.T) {
	delivery := newTestDelivery()
	consumer := mq.ConsumerFunc(func(ctx context.Context) (<-chan mq.Delivery, error) {
		deliveries := make(chan mq.Delivery, 1)
		deliveries <- mq.Delivery{
			Message: mq.Message{ID: "msg-1", Topic: "jobs"},
			Ack:     delivery.Ack,
			Nack:    delivery.Nack,
		}
		close(deliveries)
		return deliveries, nil
	})
	runner := NewRunner(HandlerFunc(func(ctx context.Context, message Message) error {
		return nil
	}))

	if err := runner.Consume(context.Background(), consumer); err != nil {
		t.Fatal(err)
	}
	if !delivery.acked {
		t.Fatal("expected delivery acked")
	}
}

func TestRunnerCloseDrainsInFlightDispatch(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	runner := NewRunner(HandlerFunc(func(ctx context.Context, message Message) error {
		close(started)
		<-release
		return nil
	}))

	dispatchDone := make(chan error, 1)
	go func() {
		dispatchDone <- runner.Dispatch(context.Background(), Message{ID: "in-flight"})
	}()
	<-started

	closeDone := make(chan error, 1)
	go func() {
		closeDone <- runner.Close()
	}()

	select {
	case err := <-closeDone:
		t.Fatalf("close returned before in-flight handler drained: %v", err)
	case <-time.After(25 * time.Millisecond):
	}

	close(release)
	if err := <-dispatchDone; err != nil {
		t.Fatalf("dispatch failed: %v", err)
	}
	if err := <-closeDone; err != nil {
		t.Fatalf("close failed: %v", err)
	}
	if err := runner.Dispatch(context.Background(), Message{ID: "after-close"}); !errors.Is(err, ErrRunnerClosed) {
		t.Fatalf("expected ErrRunnerClosed after close, got %v", err)
	}
}

type testDelivery struct {
	mu        sync.Mutex
	acked     bool
	nacked    bool
	nackCause error
	decisions []mq.Decision
}

func newTestDelivery() *testDelivery {
	return &testDelivery{}
}

func (d *testDelivery) Ack(context.Context) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.acked = true
	return nil
}

func (d *testDelivery) Nack(ctx context.Context, cause error) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.nacked = true
	d.nackCause = cause
	return nil
}

func (d *testDelivery) Decide(ctx context.Context, decision mq.Decision) error {
	d.mu.Lock()
	d.decisions = append(d.decisions, decision)
	d.mu.Unlock()
	switch decision.Action {
	case "", mq.DecisionAck:
		return d.Ack(ctx)
	default:
		return d.Nack(ctx, decision.Cause)
	}
}
