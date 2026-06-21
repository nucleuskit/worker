package worker

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	nucleuscontext "github.com/nucleuskit/core/context"
)

type Job interface {
	Run(context.Context) error
}

type JobFunc func(context.Context) error

func (fn JobFunc) Run(ctx context.Context) error {
	return fn(ctx)
}

type CronOption func(*CronRunner)

type CronRunner struct {
	job       Job
	interval  time.Duration
	immediate bool
	jobName   string
	timeout   time.Duration
	metadata  map[string]string
	hooks     []Hook

	mu     sync.Mutex
	closed bool
	done   chan struct{}
	wg     sync.WaitGroup
	seq    atomic.Uint64
}

func NewCronRunner(job Job, interval time.Duration, options ...CronOption) *CronRunner {
	runner := &CronRunner{
		job:       job,
		interval:  normalizeCronInterval(interval),
		immediate: true,
		jobName:   "job",
		done:      make(chan struct{}),
	}
	for _, option := range options {
		if option != nil {
			option(runner)
		}
	}
	return runner
}

func WithJobName(name string) CronOption {
	return func(r *CronRunner) {
		name = strings.TrimSpace(name)
		if name != "" {
			r.jobName = name
		}
	}
}

func WithImmediateRun(enabled bool) CronOption {
	return func(r *CronRunner) {
		r.immediate = enabled
	}
}

func WithJobTimeout(timeout time.Duration) CronOption {
	return func(r *CronRunner) {
		if timeout > 0 {
			r.timeout = timeout
		}
	}
}

func WithJobMetadata(metadata map[string]string) CronOption {
	return func(r *CronRunner) {
		r.metadata = cloneHeaders(metadata)
	}
}

func WithCronHook(hook Hook) CronOption {
	return func(r *CronRunner) {
		if hook != nil {
			r.hooks = append(r.hooks, hook)
		}
	}
}

func (r *CronRunner) Run(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if !r.begin() {
		return ErrRunnerClosed
	}
	defer r.finish()

	if r.immediate {
		_ = r.runJob(ctx)
	}

	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-r.stop():
			return ErrRunnerClosed
		case <-ticker.C:
			_ = r.runJob(ctx)
		}
	}
}

func (r *CronRunner) RunOnce(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if !r.begin() {
		return ErrRunnerClosed
	}
	defer r.finish()
	return r.runJob(ctx)
}

func (r *CronRunner) Close() error {
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

func (r *CronRunner) begin() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return false
	}
	r.wg.Add(1)
	return true
}

func (r *CronRunner) finish() {
	r.wg.Done()
}

func (r *CronRunner) stop() <-chan struct{} {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.done == nil {
		r.done = make(chan struct{})
	}
	return r.done
}

func (r *CronRunner) runJob(ctx context.Context) (err error) {
	startedAt := time.Now()
	runID := r.nextRunID(startedAt)
	jobCtx, traceID := ensureJobTraceID(ctx)
	var cancel context.CancelFunc
	if r.timeout > 0 {
		jobCtx, cancel = context.WithTimeout(jobCtx, r.timeout)
		defer cancel()
	}

	event := Event{
		Kind:      EventJobStarted,
		JobName:   r.jobName,
		RunID:     runID,
		TraceID:   traceID,
		StartedAt: startedAt,
		Metadata:  cloneHeaders(r.metadata),
	}
	r.emit(jobCtx, event)

	err = r.handleJob(jobCtx)
	if err == nil && r.timeout > 0 && jobCtx.Err() == context.DeadlineExceeded {
		err = context.DeadlineExceeded
	}

	endedAt := time.Now()
	event.Kind = EventJobSucceeded
	event.EndedAt = endedAt
	event.Duration = endedAt.Sub(startedAt)
	if err != nil {
		event.Kind = EventJobFailed
		event.Error = err
	}
	r.emit(jobCtx, event)
	return err
}

func (r *CronRunner) handleJob(ctx context.Context) (err error) {
	if r.job == nil {
		return nil
	}
	defer recoverPanic(&err)
	return r.job.Run(ctx)
}

func (r *CronRunner) emit(ctx context.Context, event Event) {
	for _, hook := range r.hooks {
		if hook != nil {
			hook.HandleWorkerEvent(ctx, event)
		}
	}
}

func (r *CronRunner) nextRunID(startedAt time.Time) string {
	seq := r.seq.Add(1)
	return fmt.Sprintf("%s-%d-%d", r.jobName, startedAt.UnixNano(), seq)
}

func normalizeCronInterval(interval time.Duration) time.Duration {
	if interval <= 0 {
		return time.Second
	}
	return interval
}

func ensureJobTraceID(ctx context.Context) (context.Context, string) {
	traceID := nucleuscontext.TraceID(ctx)
	if traceID != "" {
		return ctx, traceID
	}
	traceID = newTraceID()
	return nucleuscontext.WithTraceID(ctx, traceID), traceID
}

func newTraceID() string {
	var token [16]byte
	if _, err := rand.Read(token[:]); err == nil {
		return hex.EncodeToString(token[:])
	}
	return fmt.Sprintf("%032x", time.Now().UnixNano())
}
