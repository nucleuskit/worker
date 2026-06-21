package worker

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestRoutineGroupCancelsAndReturnsJoinedErrors(t *testing.T) {
	group := NewRoutineGroup(context.Background())
	group.Go(func(context.Context) error {
		return errors.New("failed")
	})
	group.Go(func(ctx context.Context) error {
		<-ctx.Done()
		return nil
	})

	err := group.Wait()
	if err == nil || !errors.Is(err, context.Canceled) && err.Error() != "failed" {
		t.Fatalf("expected routine error, got %v", err)
	}
}

func TestRoutineGroupRecoversPanic(t *testing.T) {
	group := NewRoutineGroup(context.Background())
	group.Go(func(context.Context) error {
		panic("boom")
	})
	err := group.Wait()
	if !errors.Is(err, ErrPanicRecovered) {
		t.Fatalf("expected panic recovered error, got %v", err)
	}
}

func TestBulkExecutorFlushesAtMaxItems(t *testing.T) {
	var batches [][]any
	executor := NewBulkExecutor(2, 0, func(ctx context.Context, items []any) error {
		batches = append(batches, items)
		return nil
	})
	if err := executor.Add(context.Background(), "a"); err != nil {
		t.Fatal(err)
	}
	if len(batches) != 0 {
		t.Fatalf("should not flush before max items: %#v", batches)
	}
	if err := executor.Add(context.Background(), "b"); err != nil {
		t.Fatal(err)
	}
	if len(batches) != 1 || len(batches[0]) != 2 {
		t.Fatalf("expected one flushed batch, got %#v", batches)
	}
}

func TestDelayRunsHandlerAfterDelayOrCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := Delay(ctx, time.Second, func(context.Context) error {
		t.Fatal("handler should not run after cancellation")
		return nil
	}); !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context canceled, got %v", err)
	}

	called := false
	if err := Delay(context.Background(), 0, func(context.Context) error {
		called = true
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if !called {
		t.Fatal("expected immediate delay handler")
	}
}

func TestPeriodicExecutorRunsImmediateAndStopsOnCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	calls := make(chan int, 4)
	count := 0
	executor := PeriodicExecutor{
		Interval:  time.Millisecond,
		Immediate: true,
		Handler: func(context.Context) error {
			count++
			calls <- count
			if count == 2 {
				cancel()
			}
			return nil
		},
	}

	done := make(chan error, 1)
	go func() {
		done <- executor.Run(ctx)
	}()

	for i := 1; i <= 2; i++ {
		select {
		case got := <-calls:
			if got != i {
				t.Fatalf("expected periodic call %d, got %d", i, got)
			}
		case <-time.After(time.Second):
			t.Fatalf("timed out waiting for periodic call %d", i)
		}
	}
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("expected context canceled, got %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("periodic executor did not stop after cancellation")
	}
}
