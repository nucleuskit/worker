package worker

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

func TestMapReduceMapsConcurrentlyAndReducesResults(t *testing.T) {
	inputs := []int{1, 2, 3, 4}
	var active atomic.Int32
	var maxActive atomic.Int32

	sum, err := MapReduce(context.Background(), inputs, 2,
		func(ctx context.Context, value int) (int, error) {
			now := active.Add(1)
			for {
				seen := maxActive.Load()
				if now <= seen || maxActive.CompareAndSwap(seen, now) {
					break
				}
			}
			time.Sleep(10 * time.Millisecond)
			active.Add(-1)
			return value * 2, nil
		},
		func(ctx context.Context, acc int, value int) (int, error) {
			return acc + value, nil
		},
		0,
	)

	if err != nil {
		t.Fatal(err)
	}
	if sum != 20 {
		t.Fatalf("expected reduced sum 20, got %d", sum)
	}
	if maxActive.Load() < 2 {
		t.Fatalf("expected map concurrency, max active was %d", maxActive.Load())
	}
}

func TestMapReducePropagatesErrorAndCancelsMapping(t *testing.T) {
	mapErr := errors.New("map failed")
	var started atomic.Int32
	cancelled := make(chan struct{})

	_, err := MapReduce(context.Background(), []int{1, 2}, 2,
		func(ctx context.Context, value int) (int, error) {
			started.Add(1)
			if value == 1 {
				deadline := time.After(time.Second)
				for started.Load() < 2 {
					select {
					case <-deadline:
						return 0, errors.New("timed out waiting for sibling mapper")
					default:
						time.Sleep(time.Millisecond)
					}
				}
				return 0, mapErr
			}
			<-ctx.Done()
			close(cancelled)
			return 0, ctx.Err()
		},
		func(ctx context.Context, acc int, value int) (int, error) {
			return acc + value, nil
		},
		0,
	)

	if !errors.Is(err, mapErr) {
		t.Fatalf("expected map error, got %v", err)
	}
	select {
	case <-cancelled:
	case <-time.After(time.Second):
		t.Fatal("expected sibling mapper context cancellation")
	}
}
