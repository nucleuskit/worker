package worker

import (
	"context"
	"errors"
	"testing"
	"time"

	nucleuscontext "github.com/nucleuskit/core/context"
)

func TestCronRunnerRunOnceEmitsEventsTraceAndEvidence(t *testing.T) {
	var events []Event
	var evidence []map[string]any
	ctx := nucleuscontext.WithTraceID(context.Background(), "trace-test")
	runner := NewCronRunner(JobFunc(func(ctx context.Context) error {
		if got := nucleuscontext.TraceID(ctx); got != "trace-test" {
			t.Fatalf("expected job trace id trace-test, got %q", got)
		}
		return nil
	}), time.Hour,
		WithJobName("sync-orders"),
		WithJobMetadata(map[string]string{"source": "cron"}),
		WithCronHook(HookFunc(func(ctx context.Context, event Event) {
			events = append(events, event)
		})),
		WithCronHook(EvidenceHook(EvidenceRecorderFunc(func(ctx context.Context, item map[string]any) {
			evidence = append(evidence, item)
		}))),
	)

	if err := runner.RunOnce(ctx); err != nil {
		t.Fatal(err)
	}

	if len(events) != 2 {
		t.Fatalf("expected started and succeeded events, got %#v", events)
	}
	started, succeeded := events[0], events[1]
	if started.Kind != EventJobStarted || succeeded.Kind != EventJobSucceeded {
		t.Fatalf("unexpected event kinds: %#v", events)
	}
	if started.JobName != "sync-orders" || succeeded.JobName != "sync-orders" {
		t.Fatalf("expected job name on events, got %#v", events)
	}
	if started.RunID == "" || succeeded.RunID != started.RunID {
		t.Fatalf("expected stable run id across events, got started=%q succeeded=%q", started.RunID, succeeded.RunID)
	}
	if succeeded.TraceID != "trace-test" || succeeded.Metadata["source"] != "cron" {
		t.Fatalf("unexpected event metadata: %#v", succeeded)
	}
	if succeeded.StartedAt.IsZero() || succeeded.EndedAt.IsZero() || succeeded.Duration < 0 {
		t.Fatalf("expected timing fields, got %#v", succeeded)
	}
	if len(evidence) != 2 {
		t.Fatalf("expected evidence for both events, got %#v", evidence)
	}
	if evidence[1]["capability"] != "worker" || evidence[1]["operation"] != "job_succeeded" || evidence[1]["status"] != "ok" {
		t.Fatalf("unexpected evidence map: %#v", evidence[1])
	}
	attrs, ok := evidence[1]["attributes"].(map[string]any)
	if !ok || attrs["job_name"] != "sync-orders" || attrs["run_id"] != succeeded.RunID || attrs["trace_id"] != "trace-test" {
		t.Fatalf("unexpected evidence attributes: %#v", evidence[1])
	}
}

func TestCronRunnerRunsImmediatelyAndPeriodicallyUntilCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	calls := 0
	runner := NewCronRunner(JobFunc(func(context.Context) error {
		calls++
		if calls == 2 {
			cancel()
		}
		return nil
	}), 5*time.Millisecond, WithJobName("heartbeat"))

	err := runner.Run(ctx)

	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context canceled, got %v", err)
	}
	if calls != 2 {
		t.Fatalf("expected immediate and periodic calls, got %d", calls)
	}
}

func TestCronRunnerRecoversPanicAndContinues(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	calls := 0
	var events []Event
	runner := NewCronRunner(JobFunc(func(context.Context) error {
		calls++
		if calls == 1 {
			panic("boom")
		}
		cancel()
		return nil
	}), 5*time.Millisecond, WithCronHook(HookFunc(func(ctx context.Context, event Event) {
		events = append(events, event)
	})))

	err := runner.Run(ctx)

	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context canceled, got %v", err)
	}
	var recovered bool
	var succeeded bool
	for _, event := range events {
		if event.Kind == EventJobFailed && errors.Is(event.Error, ErrPanicRecovered) {
			recovered = true
		}
		if event.Kind == EventJobSucceeded {
			succeeded = true
		}
	}
	if !recovered || !succeeded {
		t.Fatalf("expected panic recovery and later success events, got %#v", events)
	}
}

func TestCronRunnerCloseWaitsForInFlightJob(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	runner := NewCronRunner(JobFunc(func(context.Context) error {
		close(started)
		<-release
		return nil
	}), time.Hour)

	runDone := make(chan error, 1)
	go func() {
		runDone <- runner.Run(context.Background())
	}()
	<-started

	closeDone := make(chan error, 1)
	go func() {
		closeDone <- runner.Close()
	}()

	select {
	case err := <-closeDone:
		t.Fatalf("close returned before in-flight job drained: %v", err)
	case <-time.After(25 * time.Millisecond):
	}

	close(release)
	if err := <-closeDone; err != nil {
		t.Fatalf("close failed: %v", err)
	}
	if err := <-runDone; !errors.Is(err, ErrRunnerClosed) {
		t.Fatalf("expected run to stop with ErrRunnerClosed, got %v", err)
	}
	if err := runner.RunOnce(context.Background()); !errors.Is(err, ErrRunnerClosed) {
		t.Fatalf("expected RunOnce after close to reject, got %v", err)
	}
}

func TestCronRunnerTimesOutJob(t *testing.T) {
	var events []Event
	runner := NewCronRunner(JobFunc(func(ctx context.Context) error {
		<-ctx.Done()
		return ctx.Err()
	}), time.Hour,
		WithJobName("slow-job"),
		WithJobTimeout(10*time.Millisecond),
		WithCronHook(HookFunc(func(ctx context.Context, event Event) {
			events = append(events, event)
		})),
	)

	err := runner.RunOnce(context.Background())

	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected deadline exceeded, got %v", err)
	}
	if len(events) != 2 || events[1].Kind != EventJobFailed || !errors.Is(events[1].Error, context.DeadlineExceeded) {
		t.Fatalf("expected timeout failure event, got %#v", events)
	}
}

func TestNewManagerFromConfigRegistersJobsAndRunOnceByName(t *testing.T) {
	var calls []string
	manager, err := NewManagerFromConfig([]JobConfig{
		{Name: "sync-orders", Job: "sync", Schedule: "@every 10ms", Metadata: map[string]string{"source": "config"}},
		{Name: "refresh-cache", Job: "refresh", Schedule: "20ms"},
	}, map[string]Job{
		"sync": JobFunc(func(context.Context) error {
			calls = append(calls, "sync")
			return nil
		}),
		"refresh": JobFunc(func(context.Context) error {
			calls = append(calls, "refresh")
			return nil
		}),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer manager.Close()

	if err := manager.RunOnce(context.Background(), "refresh-cache"); err != nil {
		t.Fatal(err)
	}
	if err := manager.RunOnce(context.Background(), "sync-orders"); err != nil {
		t.Fatal(err)
	}

	want := []string{"refresh", "sync"}
	if len(calls) != len(want) {
		t.Fatalf("expected calls %v, got %v", want, calls)
	}
	for i := range want {
		if calls[i] != want[i] {
			t.Fatalf("expected calls %v, got %v", want, calls)
		}
	}
}

func TestNewManagerFromConfigRejectsUnknownJob(t *testing.T) {
	_, err := NewManagerFromConfig([]JobConfig{
		{Name: "missing", Job: "missing", Schedule: "@every 10ms"},
	}, map[string]Job{})

	if !errors.Is(err, ErrJobNotFound) {
		t.Fatalf("expected ErrJobNotFound, got %v", err)
	}
}

func TestManagerRunOnceRejectsUnknownRegisteredJob(t *testing.T) {
	manager, err := NewManagerFromConfig([]JobConfig{
		{Name: "known", Job: "known", Schedule: "@every 10ms"},
	}, map[string]Job{
		"known": JobFunc(func(context.Context) error { return nil }),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer manager.Close()

	if err := manager.RunOnce(context.Background(), "unknown"); !errors.Is(err, ErrJobNotFound) {
		t.Fatalf("expected ErrJobNotFound, got %v", err)
	}
}

func TestManagerCloseClosesRunners(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	manager, err := NewManagerFromConfig([]JobConfig{
		{Name: "slow", Job: "slow", Schedule: "@every 1h"},
	}, map[string]Job{
		"slow": JobFunc(func(context.Context) error {
			close(started)
			<-release
			return nil
		}),
	})
	if err != nil {
		t.Fatal(err)
	}

	runDone := make(chan error, 1)
	go func() {
		runDone <- manager.RunOnce(context.Background(), "slow")
	}()
	<-started

	closeDone := make(chan error, 1)
	go func() {
		closeDone <- manager.Close()
	}()

	select {
	case err := <-closeDone:
		t.Fatalf("close returned before in-flight job drained: %v", err)
	case <-time.After(25 * time.Millisecond):
	}

	close(release)
	if err := <-closeDone; err != nil {
		t.Fatalf("close failed: %v", err)
	}
	if err := <-runDone; err != nil {
		t.Fatalf("run once failed: %v", err)
	}
	if err := manager.RunOnce(context.Background(), "slow"); !errors.Is(err, ErrRunnerClosed) {
		t.Fatalf("expected ErrRunnerClosed after manager close, got %v", err)
	}
}

func TestManagerRunsCronExpressionSchedule(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 1500*time.Millisecond)
	defer cancel()
	called := make(chan struct{}, 1)
	manager, err := NewManagerFromConfig([]JobConfig{
		{Name: "cron-expression", Job: "cron", Schedule: "*/1 * * * * *"},
	}, map[string]Job{
		"cron": JobFunc(func(context.Context) error {
			select {
			case called <- struct{}{}:
			default:
			}
			cancel()
			return nil
		}),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer manager.Close()

	err = manager.Run(ctx)

	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context canceled after cron expression run, got %v", err)
	}
	select {
	case <-called:
	default:
		t.Fatal("expected cron expression job to run")
	}
}

func TestNewManagerFromConfigRejectsInvalidCronExpression(t *testing.T) {
	_, err := NewManagerFromConfig([]JobConfig{
		{Name: "bad-cron", Job: "known", Schedule: "not a cron expression"},
	}, map[string]Job{
		"known": JobFunc(func(context.Context) error { return nil }),
	})

	if !errors.Is(err, ErrInvalidSchedule) {
		t.Fatalf("expected ErrInvalidSchedule, got %v", err)
	}
}
