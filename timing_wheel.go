package worker

import (
	"sync"
	"sync/atomic"
	"time"
)

type TimingWheel struct {
	tick    time.Duration
	buckets [][]*TimingTask

	mu      sync.Mutex
	current int
	stopped bool
	done    chan struct{}
	wg      sync.WaitGroup
}

type TimingTask struct {
	fn     func()
	rounds int
	state  atomic.Int32
}

func NewTimingWheel(tick time.Duration, slots int) *TimingWheel {
	if tick <= 0 {
		tick = time.Millisecond
	}
	if slots <= 0 {
		slots = 64
	}
	wheel := &TimingWheel{
		tick:    tick,
		buckets: make([][]*TimingTask, slots),
		done:    make(chan struct{}),
	}
	wheel.wg.Add(1)
	go wheel.run()
	return wheel
}

func (w *TimingWheel) Schedule(delay time.Duration, fn func()) *TimingTask {
	if w == nil || fn == nil {
		return nil
	}
	ticks := int((delay + w.tick - 1) / w.tick)
	if ticks < 1 {
		ticks = 1
	}
	task := &TimingTask{
		fn:     fn,
		rounds: (ticks - 1) / len(w.buckets),
	}

	w.mu.Lock()
	defer w.mu.Unlock()
	if w.stopped {
		return nil
	}
	slot := (w.current + ticks) % len(w.buckets)
	w.buckets[slot] = append(w.buckets[slot], task)
	return task
}

func (w *TimingWheel) Cancel(task *TimingTask) bool {
	if w == nil || task == nil {
		return false
	}
	return task.state.CompareAndSwap(0, 1)
}

func (w *TimingWheel) Stop() {
	if w == nil {
		return
	}
	w.mu.Lock()
	if w.stopped {
		w.mu.Unlock()
		w.wg.Wait()
		return
	}
	w.stopped = true
	close(w.done)
	for i := range w.buckets {
		for _, task := range w.buckets[i] {
			task.state.CompareAndSwap(0, 1)
		}
		w.buckets[i] = nil
	}
	w.mu.Unlock()
	w.wg.Wait()
}

func (w *TimingWheel) run() {
	defer w.wg.Done()
	ticker := time.NewTicker(w.tick)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			w.advance()
		case <-w.done:
			return
		}
	}
}

func (w *TimingWheel) advance() {
	var ready []*TimingTask
	w.mu.Lock()
	if w.stopped {
		w.mu.Unlock()
		return
	}
	w.current = (w.current + 1) % len(w.buckets)
	bucket := w.buckets[w.current]
	w.buckets[w.current] = nil
	remaining := bucket[:0]
	for _, task := range bucket {
		if task.state.Load() != 0 {
			continue
		}
		if task.rounds > 0 {
			task.rounds--
			remaining = append(remaining, task)
			continue
		}
		if task.state.CompareAndSwap(0, 2) {
			ready = append(ready, task)
		}
	}
	if len(remaining) > 0 {
		w.buckets[w.current] = append(w.buckets[w.current], remaining...)
	}
	w.mu.Unlock()

	for _, task := range ready {
		go task.fn()
	}
}
