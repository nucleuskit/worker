package worker

import (
	"sync/atomic"
	"testing"
	"time"
)

func TestTimingWheelSchedulesAndCancelsTasks(t *testing.T) {
	wheel := NewTimingWheel(5*time.Millisecond, 8)
	defer wheel.Stop()

	ran := make(chan string, 2)
	_ = wheel.Schedule(15*time.Millisecond, func() {
		ran <- "kept"
	})
	cancelled := wheel.Schedule(10*time.Millisecond, func() {
		ran <- "cancelled"
	})
	if !wheel.Cancel(cancelled) {
		t.Fatal("expected cancel to report true")
	}
	if wheel.Cancel(cancelled) {
		t.Fatal("expected second cancel to report false")
	}

	select {
	case got := <-ran:
		if got != "kept" {
			t.Fatalf("expected kept task to run, got %q", got)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for scheduled task")
	}

	select {
	case got := <-ran:
		t.Fatalf("cancelled task should not run, got %q", got)
	case <-time.After(30 * time.Millisecond):
	}
}

func TestTimingWheelStopPreventsPendingAndNewTasks(t *testing.T) {
	wheel := NewTimingWheel(10*time.Millisecond, 4)
	var calls atomic.Int32
	task := wheel.Schedule(100*time.Millisecond, func() {
		calls.Add(1)
	})
	wheel.Stop()

	if wheel.Cancel(task) {
		t.Fatal("expected stop to make pending task no longer cancellable")
	}
	if got := wheel.Schedule(time.Millisecond, func() {}); got != nil {
		t.Fatalf("expected schedule after stop to return nil, got %#v", got)
	}
	time.Sleep(30 * time.Millisecond)
	if calls.Load() != 0 {
		t.Fatalf("pending task ran after stop")
	}
}
