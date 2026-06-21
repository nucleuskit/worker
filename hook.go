package worker

import (
	"context"
	"errors"
	"time"

	"github.com/nucleuskit/nucleus/cap/mq"
)

type EventKind string

const (
	EventMessageStarted      EventKind = "message_started"
	EventMessageSucceeded    EventKind = "message_succeeded"
	EventMessageFailed       EventKind = "message_failed"
	EventMessageRetried      EventKind = "message_retried"
	EventMessageAcked        EventKind = "message_acked"
	EventMessageNacked       EventKind = "message_nacked"
	EventMessageDeadLettered EventKind = "message_dead_lettered"
	EventBatchStarted        EventKind = "batch_started"
	EventBatchSucceeded      EventKind = "batch_succeeded"
	EventBatchFailed         EventKind = "batch_failed"
	EventJobStarted          EventKind = "job_started"
	EventJobSucceeded        EventKind = "job_succeeded"
	EventJobFailed           EventKind = "job_failed"
	EventJobRetried          EventKind = "job_retried"
	EventJobSkipped          EventKind = "job_skipped"
)

type Event struct {
	Kind      EventKind
	Message   Message
	Messages  []Message
	JobName   string
	RunID     string
	TraceID   string
	Decision  DeliveryDecision
	Attempt   int
	Error     error
	StartedAt time.Time
	EndedAt   time.Time
	Duration  time.Duration
	Metadata  map[string]string
}

type Hook interface {
	HandleWorkerEvent(context.Context, Event)
}

type HookFunc func(context.Context, Event)

func (fn HookFunc) HandleWorkerEvent(ctx context.Context, event Event) {
	fn(ctx, event)
}

func WithHook(hook Hook) RunnerOption {
	return func(r *Runner) {
		if hook != nil {
			r.hooks = append(r.hooks, hook)
		}
	}
}

func WithRetryPolicy(policy mq.RetryPolicy) RunnerOption {
	return func(r *Runner) {
		r.retryPolicy = policy
	}
}

func WithDeadLetterPolicy(policy mq.DeadLetterPolicy) RunnerOption {
	return func(r *Runner) {
		r.deadLetterPolicy = policy
	}
}

type EvidenceRecorder interface {
	RecordEvidence(context.Context, map[string]any)
}

type EvidenceRecorderFunc func(context.Context, map[string]any)

func (fn EvidenceRecorderFunc) RecordEvidence(ctx context.Context, evidence map[string]any) {
	if fn == nil {
		return
	}
	fn(ctx, evidence)
}

func EvidenceHook(recorder EvidenceRecorder) Hook {
	return HookFunc(func(ctx context.Context, event Event) {
		if recorder == nil || !isJobEvent(event.Kind) {
			return
		}
		recorder.RecordEvidence(ctx, EventCapabilityEvidence(event))
	})
}

func EventCapabilityEvidence(event Event) map[string]any {
	evidence := map[string]any{
		"capability": "worker",
		"operation":  string(event.Kind),
		"status":     eventEvidenceStatus(event),
		"attributes": eventEvidenceAttributes(event),
	}
	if event.JobName != "" {
		evidence["resource"] = event.JobName
	}
	if event.Duration > 0 {
		evidence["duration_ms"] = float64(event.Duration) / float64(time.Millisecond)
	}
	if event.Error != nil {
		evidence["error"] = event.Error.Error()
	}
	return evidence
}

func (r *Runner) emit(ctx context.Context, event Event) {
	for _, hook := range r.hooks {
		if hook != nil {
			hook.HandleWorkerEvent(ctx, event)
		}
	}
}

func isJobEvent(kind EventKind) bool {
	switch kind {
	case EventJobStarted, EventJobSucceeded, EventJobFailed, EventJobRetried, EventJobSkipped:
		return true
	default:
		return false
	}
}

func eventEvidenceStatus(event Event) string {
	switch event.Kind {
	case EventJobSkipped:
		return "skipped"
	case EventJobFailed:
		if errors.Is(event.Error, context.DeadlineExceeded) {
			return "timeout"
		}
		return "error"
	default:
		if event.Error != nil {
			return "error"
		}
		return "ok"
	}
}

func eventEvidenceAttributes(event Event) map[string]any {
	attributes := map[string]any{
		"event_kind": string(event.Kind),
	}
	if event.JobName != "" {
		attributes["job_name"] = event.JobName
	}
	if event.RunID != "" {
		attributes["run_id"] = event.RunID
	}
	if event.TraceID != "" {
		attributes["trace_id"] = event.TraceID
	}
	if !event.StartedAt.IsZero() {
		attributes["started_at"] = event.StartedAt.UTC().Format(time.RFC3339Nano)
	}
	if !event.EndedAt.IsZero() {
		attributes["ended_at"] = event.EndedAt.UTC().Format(time.RFC3339Nano)
	}
	if len(event.Metadata) > 0 {
		attributes["metadata"] = cloneHeaders(event.Metadata)
	}
	return attributes
}
