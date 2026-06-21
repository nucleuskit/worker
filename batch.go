package worker

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/nucleuskit/cap/mq"
)

type BatchPolicy struct {
	MaxMessages    int
	FlushInterval  time.Duration
	MaxConcurrency int
}

type RunnerOption func(*Runner)

func WithBatchPolicy(policy BatchPolicy) RunnerOption {
	return func(r *Runner) {
		r.batchPolicy = normalizeBatchPolicy(policy)
		r.batchEnabled = true
	}
}

type BatchHandler interface {
	Handler
	HandleBatch(context.Context, []Message) error
}

type BatchResultHandler interface {
	Handler
	HandleBatchResult(context.Context, []Message) (BatchResult, error)
}

type BatchHandlerFunc func(context.Context, []Message) error

func (fn BatchHandlerFunc) Handle(ctx context.Context, message Message) error {
	return fn(ctx, []Message{message})
}

func (fn BatchHandlerFunc) HandleBatch(ctx context.Context, messages []Message) error {
	return fn(ctx, messages)
}

type BatchAction string

const (
	BatchActionAck        BatchAction = "ack"
	BatchActionNack       BatchAction = "nack"
	BatchActionRetry      BatchAction = "retry"
	BatchActionDeadLetter BatchAction = "dead_letter"
)

type BatchDecision struct {
	Index      int
	Action     BatchAction
	Cause      error
	Message    Message
	RetryAfter time.Duration
	DeadLetter mq.DeadLetterMetadata
}

type BatchResult struct {
	Decisions []BatchDecision
}

type batchDelivery struct {
	delivery mq.Delivery
	message  Message
}

type batchExecutor struct {
	runner *Runner
	sem    chan struct{}
	wg     sync.WaitGroup
	mu     sync.Mutex
	err    error
}

func newBatchExecutor(runner *Runner) *batchExecutor {
	concurrency := runner.batchPolicy.MaxConcurrency
	if concurrency <= 1 {
		concurrency = 1
	}
	return &batchExecutor{
		runner: runner,
		sem:    make(chan struct{}, concurrency),
	}
}

func (e *batchExecutor) submit(ctx context.Context, batch []batchDelivery) error {
	if len(batch) == 0 {
		return nil
	}
	if err := e.firstErr(); err != nil {
		return err
	}
	batch = append([]batchDelivery(nil), batch...)
	if cap(e.sem) == 1 {
		if err := e.runner.handleDeliveryBatch(ctx, batch); err != nil {
			e.recordErr(err)
			return err
		}
		return nil
	}
	select {
	case e.sem <- struct{}{}:
	case <-e.runner.stop():
		return ErrRunnerClosed
	}
	e.wg.Add(1)
	go func() {
		defer e.wg.Done()
		defer func() { <-e.sem }()
		if err := e.runner.handleDeliveryBatch(ctx, batch); err != nil {
			e.recordErr(err)
		}
	}()
	return nil
}

func (e *batchExecutor) wait() error {
	e.wg.Wait()
	return e.firstErr()
}

func (e *batchExecutor) recordErr(err error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.err == nil {
		e.err = err
	}
}

func (e *batchExecutor) firstErr() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.err
}

func (r *Runner) consumeBatch(ctx context.Context, deliveries <-chan mq.Delivery) error {
	executor := newBatchExecutor(r)
	batch := make([]batchDelivery, 0, r.batchPolicy.MaxMessages)
	timer := newBatchTimer(r.batchPolicy.FlushInterval)
	defer timer.Stop()

	flush := func(flushCtx context.Context) error {
		if len(batch) == 0 {
			return nil
		}
		err := executor.submit(flushCtx, batch)
		batch = batch[:0]
		timer.Reset()
		return err
	}

	for {
		select {
		case <-ctx.Done():
			drainDeliveries(deliveries, &batch, r.batchPolicy.MaxMessages, func() {
				_ = flush(context.WithoutCancel(ctx))
			})
			if err := flush(context.WithoutCancel(ctx)); err != nil {
				_ = executor.wait()
				return err
			}
			if err := executor.wait(); err != nil {
				return err
			}
			return ctx.Err()
		case <-r.stop():
			if err := flush(context.WithoutCancel(ctx)); err != nil {
				_ = executor.wait()
				return err
			}
			if err := executor.wait(); err != nil {
				return err
			}
			return ErrRunnerClosed
		case <-timer.C():
			if err := flush(drainContext(ctx)); err != nil {
				_ = executor.wait()
				return err
			}
		case delivery, ok := <-deliveries:
			if !ok {
				if err := flush(drainContext(ctx)); err != nil {
					_ = executor.wait()
					return err
				}
				if err := executor.wait(); err != nil {
					return err
				}
				if err := ctx.Err(); err != nil {
					return err
				}
				return nil
			}
			batch = append(batch, batchDelivery{
				delivery: delivery,
				message:  fromMQMessage(delivery.Message),
			})
			if len(batch) >= r.batchPolicy.MaxMessages {
				if err := flush(drainContext(ctx)); err != nil {
					_ = executor.wait()
					return err
				}
			} else {
				timer.Start()
			}
		}
	}
}

func drainContext(ctx context.Context) context.Context {
	if ctx.Err() == nil {
		return ctx
	}
	return context.WithoutCancel(ctx)
}

func (r *Runner) handleDeliveryBatch(ctx context.Context, batch []batchDelivery) error {
	if !r.begin() {
		return ErrRunnerClosed
	}
	defer r.finish()
	messages := make([]Message, len(batch))
	for i, item := range batch {
		messages[i] = item.message
	}
	attempt := maxBatchAttempt(batch)
	for {
		r.emit(ctx, Event{Kind: EventBatchStarted, Messages: messages, Attempt: attempt})
		started := time.Now()
		result, err := r.handleBatchResult(ctx, messages)
		var applyErr error
		if err == nil || len(result.Decisions) > 0 {
			applyErr = applyBatchResultWithPolicy(ctx, batch, result, err, r.deadLetterPolicy)
		}
		event := Event{Kind: EventBatchSucceeded, Messages: messages, Attempt: attempt, Duration: time.Since(started)}
		if err != nil || applyErr != nil {
			event.Kind = EventBatchFailed
			event.Error = errors.Join(err, applyErr)
		}
		r.emit(ctx, event)
		if applyErr != nil {
			return applyErr
		}
		if err == nil {
			return nil
		}
		if r.shouldRetry(attempt, err) {
			retryAfter := r.retryPolicy.Backoff(attempt)
			r.emit(ctx, Event{Kind: EventMessageRetried, Messages: messages, Attempt: attempt, Error: err})
			if sleepErr := sleepBackoff(ctx, retryAfter); sleepErr != nil {
				return errors.Join(err, sleepErr)
			}
			attempt++
			continue
		}
		if r.shouldDeadLetter(attempt, err) {
			return deadLetterBatch(ctx, batch, err, r.deadLetterPolicy, attempt)
		}
		return nackBatch(ctx, batch, err)
	}
}

func (r *Runner) handleBatch(ctx context.Context, messages []Message) error {
	if len(messages) == 0 || r.handler == nil {
		return nil
	}
	if r.batchHandler != nil {
		return r.batchHandler.HandleBatch(ctx, messages)
	}
	for _, message := range messages {
		if err := r.handler.Handle(ctx, message); err != nil {
			return err
		}
	}
	return nil
}

func (r *Runner) handleBatchResult(ctx context.Context, messages []Message) (result BatchResult, err error) {
	defer recoverPanic(&err)
	if resultHandler, ok := r.handler.(BatchResultHandler); ok {
		return resultHandler.HandleBatchResult(ctx, messages)
	}
	return BatchResult{}, r.handleBatch(ctx, messages)
}

func ackBatch(ctx context.Context, batch []batchDelivery) error {
	var joined error
	for _, item := range batch {
		if err := ackDelivery(ctx, item.delivery); err != nil {
			joined = errors.Join(joined, err)
		}
	}
	return joined
}

func nackBatch(ctx context.Context, batch []batchDelivery, cause error) error {
	joined := cause
	for _, item := range batch {
		if err := nackDelivery(ctx, item.delivery, cause); err != nil {
			joined = errors.Join(joined, err)
		}
	}
	return joined
}

func applyBatchResult(ctx context.Context, batch []batchDelivery, result BatchResult, handlerErr error) error {
	return applyBatchResultWithPolicy(ctx, batch, result, handlerErr, mq.DeadLetterPolicy{})
}

func applyBatchResultWithPolicy(ctx context.Context, batch []batchDelivery, result BatchResult, handlerErr error, deadLetterPolicy mq.DeadLetterPolicy) error {
	if len(result.Decisions) == 0 {
		if handlerErr != nil {
			return nackBatch(ctx, batch, handlerErr)
		}
		return ackBatch(ctx, batch)
	}
	decisions := make([]BatchDecision, len(batch))
	for i, item := range batch {
		decisions[i] = BatchDecision{Index: i, Action: BatchActionAck, Message: item.message}
	}
	var invalid error
	for _, decision := range result.Decisions {
		if decision.Index < 0 || decision.Index >= len(batch) {
			invalid = errors.Join(invalid, errors.New("worker batch decision index out of range"))
			continue
		}
		if decision.Action == "" {
			decision.Action = BatchActionAck
		}
		decision.Message = batch[decision.Index].message
		decisions[decision.Index] = decision
	}
	if invalid != nil {
		return nackBatch(ctx, batch, errors.Join(handlerErr, invalid))
	}
	var joined error
	for i, decision := range decisions {
		switch decision.Action {
		case BatchActionAck:
			joined = errors.Join(joined, ackDelivery(ctx, batch[i].delivery))
		case BatchActionNack:
			cause := decision.Cause
			if cause == nil {
				cause = handlerErr
			}
			if cause == nil {
				cause = errors.New("worker batch message nacked")
			}
			joined = errors.Join(joined, nackDelivery(ctx, batch[i].delivery, cause))
		case BatchActionRetry:
			cause := decision.Cause
			if cause == nil {
				cause = handlerErr
			}
			if cause == nil {
				cause = errors.New("worker batch message retried")
			}
			joined = errors.Join(joined, retryDelivery(ctx, batch[i].delivery, cause, decision.RetryAfter))
		case BatchActionDeadLetter:
			cause := decision.Cause
			if cause == nil {
				cause = handlerErr
			}
			if cause == nil {
				cause = errors.New("worker batch message dead-lettered")
			}
			metadata := decision.DeadLetter
			if metadata.Topic == "" {
				metadata = deadLetterMetadata(batch[i].delivery.Message, deadLetterPolicy, cause, nil)
			}
			joined = errors.Join(joined, deadLetterDelivery(ctx, batch[i].delivery, cause, metadata))
		default:
			joined = errors.Join(joined, nackBatch(ctx, batch, errors.New("worker batch decision action unsupported")))
			return errors.Join(handlerErr, joined)
		}
	}
	return errors.Join(handlerErr, joined)
}

func ackDelivery(ctx context.Context, delivery mq.Delivery) error {
	return delivery.AckMessage(ctx)
}

func nackDelivery(ctx context.Context, delivery mq.Delivery, cause error) error {
	return delivery.NackMessage(ctx, cause)
}

func retryDelivery(ctx context.Context, delivery mq.Delivery, cause error, retryAfter time.Duration) error {
	return delivery.RetryMessage(ctx, cause, retryAfter)
}

func deadLetterDelivery(ctx context.Context, delivery mq.Delivery, cause error, metadata mq.DeadLetterMetadata) error {
	return delivery.DeadLetterMessage(ctx, cause, metadata)
}

func deadLetterBatch(ctx context.Context, batch []batchDelivery, cause error, policy mq.DeadLetterPolicy, attempt int) error {
	joined := cause
	for _, item := range batch {
		message := item.delivery.Message
		message.Metadata.DeliveryAttempt = attempt
		metadata := deadLetterMetadata(message, policy, cause, nil)
		if err := deadLetterDelivery(ctx, item.delivery, cause, metadata); err != nil {
			joined = errors.Join(joined, err)
		}
	}
	return joined
}

func maxBatchAttempt(batch []batchDelivery) int {
	attempt := 1
	for _, item := range batch {
		if item.delivery.Message.Metadata.DeliveryAttempt > attempt {
			attempt = item.delivery.Message.Metadata.DeliveryAttempt
		}
	}
	return attempt
}

func normalizeBatchPolicy(policy BatchPolicy) BatchPolicy {
	if policy.MaxMessages <= 0 {
		policy.MaxMessages = 1
	}
	if policy.MaxConcurrency <= 0 {
		policy.MaxConcurrency = 1
	}
	return policy
}

func drainDeliveries(deliveries <-chan mq.Delivery, batch *[]batchDelivery, maxMessages int, flush func()) {
	for {
		select {
		case delivery, ok := <-deliveries:
			if !ok {
				return
			}
			*batch = append(*batch, batchDelivery{
				delivery: delivery,
				message:  fromMQMessage(delivery.Message),
			})
			if len(*batch) >= maxMessages {
				flush()
			}
		default:
			return
		}
	}
}

type batchTimer struct {
	interval time.Duration
	timer    *time.Timer
}

func newBatchTimer(interval time.Duration) *batchTimer {
	return &batchTimer{interval: interval}
}

func (t *batchTimer) C() <-chan time.Time {
	if t.timer == nil {
		return nil
	}
	return t.timer.C
}

func (t *batchTimer) Start() {
	if t.interval <= 0 || t.timer != nil {
		return
	}
	t.timer = time.NewTimer(t.interval)
}

func (t *batchTimer) Reset() {
	if t.timer == nil {
		return
	}
	if !t.timer.Stop() {
		select {
		case <-t.timer.C:
		default:
		}
	}
	t.timer = nil
}

func (t *batchTimer) Stop() {
	t.Reset()
}
