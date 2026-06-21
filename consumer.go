package worker

import (
	"context"
	"errors"
	"fmt"
	"runtime/debug"
	"sync"
	"time"

	"github.com/nucleuskit/cap/mq"
)

var ErrRunnerClosed = errors.New("worker runner closed")
var ErrPanicRecovered = errors.New("worker panic recovered")

type Runner struct {
	handler          Handler
	batchHandler     BatchHandler
	batchPolicy      BatchPolicy
	batchEnabled     bool
	retryPolicy      mq.RetryPolicy
	deadLetterPolicy mq.DeadLetterPolicy
	hooks            []Hook
	mu               sync.Mutex
	closed           bool
	wg               sync.WaitGroup
	done             chan struct{}
}

type PanicError struct {
	Value any
	Stack []byte
}

func (e PanicError) Error() string {
	return fmt.Sprintf("%v: %v", ErrPanicRecovered, e.Value)
}

func (e PanicError) Unwrap() error {
	return ErrPanicRecovered
}

type DeliveryAction string

const (
	DeliveryActionAck        DeliveryAction = "ack"
	DeliveryActionNack       DeliveryAction = "nack"
	DeliveryActionRetry      DeliveryAction = "retry"
	DeliveryActionDeadLetter DeliveryAction = "dead_letter"
)

type DeliveryDecision struct {
	Action     DeliveryAction
	Cause      error
	RetryAfter time.Duration
	Metadata   map[string]string
	DeadLetter mq.DeadLetterMetadata
}

type DecisionHandler interface {
	Handler
	HandleDecision(context.Context, Message) (DeliveryDecision, error)
}

type DecisionHandlerFunc func(context.Context, Message) (DeliveryDecision, error)

func (fn DecisionHandlerFunc) Handle(ctx context.Context, message Message) error {
	_, err := fn(ctx, message)
	return err
}

func (fn DecisionHandlerFunc) HandleDecision(ctx context.Context, message Message) (DeliveryDecision, error) {
	return fn(ctx, message)
}

func NewRunner(handler Handler, options ...RunnerOption) *Runner {
	runner := &Runner{handler: handler, done: make(chan struct{})}
	if batchHandler, ok := handler.(BatchHandler); ok {
		runner.batchHandler = batchHandler
	}
	for _, option := range options {
		if option != nil {
			option(runner)
		}
	}
	return runner
}

func (r *Runner) Dispatch(ctx context.Context, message Message) error {
	if !r.begin() {
		return ErrRunnerClosed
	}
	defer r.finish()
	r.emit(ctx, Event{Kind: EventMessageStarted, Message: message, Attempt: attemptOrOne(message.Metadata.DeliveryAttempt)})
	started := time.Now()
	err := r.handleMessage(ctx, message)
	event := Event{Kind: EventMessageSucceeded, Message: message, Attempt: attemptOrOne(message.Metadata.DeliveryAttempt), Duration: time.Since(started)}
	if err != nil {
		event.Kind = EventMessageFailed
		event.Error = err
	}
	r.emit(ctx, event)
	return err
}

func (r *Runner) Consume(ctx context.Context, consumer mq.Consumer) error {
	deliveries, err := consumer.Consume(ctx)
	if err != nil {
		return err
	}
	if r.batchEnabled {
		return r.consumeBatch(ctx, deliveries)
	}
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-r.stop():
			return ErrRunnerClosed
		case delivery, ok := <-deliveries:
			if !ok {
				return nil
			}
			if err := r.HandleDelivery(ctx, delivery); err != nil {
				return err
			}
		}
	}
}

func (r *Runner) HandleDelivery(ctx context.Context, delivery mq.Delivery) error {
	if !r.begin() {
		return ErrRunnerClosed
	}
	defer r.finish()

	message := fromMQMessage(delivery.Message)
	attempt := attemptOrOne(delivery.Message.Metadata.DeliveryAttempt)
	for {
		r.emit(ctx, Event{Kind: EventMessageStarted, Message: message, Attempt: attempt})
		started := time.Now()
		decision, err := r.handleMessageDecision(ctx, message)
		decision = normalizeDeliveryDecision(decision, err)
		event := Event{Kind: EventMessageSucceeded, Message: message, Decision: decision, Attempt: attempt, Duration: time.Since(started)}
		if err != nil || decision.Action != DeliveryActionAck {
			event.Kind = EventMessageFailed
			event.Error = firstError(err, decision.Cause)
		}
		r.emit(ctx, event)

		if decision.Action != DeliveryActionRetry && r.shouldRetry(attempt, firstError(err, decision.Cause)) {
			cause := firstError(err, decision.Cause)
			retryAfter := decision.RetryAfter
			if retryAfter <= 0 {
				retryAfter = r.retryPolicy.Backoff(attempt)
			}
			r.emit(ctx, Event{Kind: EventMessageRetried, Message: message, Decision: decision, Attempt: attempt, Error: cause})
			if sleepErr := sleepBackoff(ctx, retryAfter); sleepErr != nil {
				return errors.Join(cause, sleepErr)
			}
			attempt++
			message.Metadata.DeliveryAttempt = attempt
			continue
		}

		if decision.Action == DeliveryActionDeadLetter || r.shouldDeadLetter(attempt, firstError(err, decision.Cause)) {
			cause := firstError(err, decision.Cause)
			if cause == nil {
				cause = errors.New("worker delivery dead-lettered")
			}
			decision.Action = DeliveryActionDeadLetter
			if decision.Cause == nil {
				decision.Cause = cause
			}
			if decision.DeadLetter.Topic == "" {
				deadLetterMessage := delivery.Message
				deadLetterMessage.Metadata.DeliveryAttempt = attempt
				decision.DeadLetter = deadLetterMetadata(deadLetterMessage, r.deadLetterPolicy, cause, decision.Metadata)
			}
			applyErr := applyDeliveryDecision(ctx, delivery, decision)
			r.emit(ctx, Event{Kind: EventMessageDeadLettered, Message: message, Decision: decision, Attempt: attempt, Error: cause})
			return errors.Join(cause, applyErr)
		}

		applyErr := applyDeliveryDecision(ctx, delivery, decision)
		switch decision.Action {
		case DeliveryActionAck:
			r.emit(ctx, Event{Kind: EventMessageAcked, Message: message, Decision: decision, Attempt: attempt, Error: applyErr})
			return applyErr
		case DeliveryActionRetry:
			cause := firstError(err, decision.Cause)
			r.emit(ctx, Event{Kind: EventMessageRetried, Message: message, Decision: decision, Attempt: attempt, Error: cause})
			return errors.Join(cause, applyErr)
		default:
			cause := firstError(err, decision.Cause)
			if cause == nil {
				cause = errors.New("worker delivery nacked")
			}
			r.emit(ctx, Event{Kind: EventMessageNacked, Message: message, Decision: decision, Attempt: attempt, Error: cause})
			return errors.Join(cause, applyErr)
		}
	}
}

func (r *Runner) Close() error {
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		r.wg.Wait()
		return nil
	}
	r.closed = true
	if r.done == nil {
		r.done = make(chan struct{})
	}
	close(r.done)
	r.mu.Unlock()
	r.wg.Wait()
	return nil
}

func (r *Runner) begin() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return false
	}
	r.wg.Add(1)
	return true
}

func (r *Runner) finish() {
	r.wg.Done()
}

func (r *Runner) stop() <-chan struct{} {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.done == nil {
		r.done = make(chan struct{})
	}
	return r.done
}

func fromMQMessage(message mq.Message) Message {
	headers := message.Headers
	if headers == nil {
		headers = message.Header
	}
	return Message{
		ID:       message.ID,
		Topic:    message.Topic,
		Key:      message.Key,
		Payload:  message.Body,
		Headers:  cloneHeaders(headers),
		Metadata: message.Metadata,
	}
}

func cloneHeaders(headers map[string]string) map[string]string {
	if len(headers) == 0 {
		return nil
	}
	cloned := make(map[string]string, len(headers))
	for key, value := range headers {
		cloned[key] = value
	}
	return cloned
}

func (r *Runner) handleMessage(ctx context.Context, message Message) (err error) {
	if r.handler == nil {
		return nil
	}
	defer recoverPanic(&err)
	return r.handler.Handle(ctx, message)
}

func (r *Runner) handleMessageDecision(ctx context.Context, message Message) (decision DeliveryDecision, err error) {
	if r.handler == nil {
		return DeliveryDecision{Action: DeliveryActionAck}, nil
	}
	defer recoverPanic(&err)
	if decisionHandler, ok := r.handler.(DecisionHandler); ok {
		return decisionHandler.HandleDecision(ctx, message)
	}
	err = r.handler.Handle(ctx, message)
	return DeliveryDecision{}, err
}

func recoverPanic(err *error) {
	if value := recover(); value != nil {
		*err = PanicError{Value: value, Stack: debug.Stack()}
	}
}

func normalizeDeliveryDecision(decision DeliveryDecision, err error) DeliveryDecision {
	if decision.Action == "" {
		if err != nil {
			decision.Action = DeliveryActionNack
		} else {
			decision.Action = DeliveryActionAck
		}
	}
	if decision.Cause == nil {
		decision.Cause = err
	}
	return decision
}

func applyDeliveryDecision(ctx context.Context, delivery mq.Delivery, decision DeliveryDecision) error {
	switch decision.Action {
	case "", DeliveryActionAck:
		return delivery.AckMessage(ctx)
	case DeliveryActionRetry:
		return delivery.RetryMessage(ctx, decision.Cause, decision.RetryAfter)
	case DeliveryActionDeadLetter:
		return delivery.DeadLetterMessage(ctx, decision.Cause, decision.DeadLetter)
	default:
		return delivery.NackMessage(ctx, decision.Cause)
	}
}

func (r *Runner) shouldRetry(attempt int, err error) bool {
	return r.retryPolicy.ShouldRetry(attempt, err)
}

func (r *Runner) shouldDeadLetter(attempt int, err error) bool {
	if err == nil {
		return false
	}
	if r.deadLetterPolicy.Topic == "" && r.deadLetterPolicy.MaxAttempts <= 0 {
		return false
	}
	if r.deadLetterPolicy.MaxAttempts <= 0 {
		return true
	}
	return attempt >= r.deadLetterPolicy.MaxAttempts
}

func sleepBackoff(ctx context.Context, delay time.Duration) error {
	if delay <= 0 {
		return nil
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func firstError(errors ...error) error {
	for _, err := range errors {
		if err != nil {
			return err
		}
	}
	return nil
}

func attemptOrOne(attempt int) int {
	if attempt <= 0 {
		return 1
	}
	return attempt
}

func deadLetterMetadata(message mq.Message, policy mq.DeadLetterPolicy, cause error, attributes map[string]string) mq.DeadLetterMetadata {
	topic := policy.Topic
	if topic == "" {
		topic = message.Topic + ".dlq"
	}
	metadata := mq.DeadLetterMetadata{
		Topic:             topic,
		Reason:            policy.Reason,
		OriginalTopic:     message.Topic,
		OriginalGroup:     message.Metadata.Group,
		OriginalPartition: message.Metadata.Partition,
		OriginalOffset:    message.Metadata.Offset,
		Attempts:          attemptOrOne(message.Metadata.DeliveryAttempt),
		FailedAt:          time.Now(),
		Attributes:        cloneHeaders(policy.Metadata),
	}
	if metadata.Reason == "" && cause != nil {
		metadata.Reason = cause.Error()
	}
	for key, value := range attributes {
		if metadata.Attributes == nil {
			metadata.Attributes = map[string]string{}
		}
		metadata.Attributes[key] = value
	}
	return metadata
}
