package worker

import (
	"context"
	"errors"
	"sync"
	"time"
)

type RoutineGroup struct {
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
	mu     sync.Mutex
	err    error
}

type BulkExecutor struct {
	mu       sync.Mutex
	items    []any
	max      int
	interval time.Duration
	handler  func(context.Context, []any) error
	timer    *time.Timer
}

type PeriodicExecutor struct {
	Interval  time.Duration
	Immediate bool
	Handler   func(context.Context) error
}

func NewRoutineGroup(ctx context.Context) *RoutineGroup {
	if ctx == nil {
		ctx = context.Background()
	}
	ctx, cancel := context.WithCancel(ctx)
	return &RoutineGroup{ctx: ctx, cancel: cancel}
}

func (g *RoutineGroup) Go(fn func(context.Context) error) {
	if g == nil || fn == nil {
		return
	}
	g.wg.Add(1)
	go func() {
		defer g.wg.Done()
		defer func() {
			if value := recover(); value != nil {
				g.record(PanicError{Value: value})
				g.cancel()
			}
		}()
		if err := fn(g.ctx); err != nil {
			g.record(err)
			g.cancel()
		}
	}()
}

func (g *RoutineGroup) Wait() error {
	if g == nil {
		return nil
	}
	g.wg.Wait()
	g.cancel()
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.err
}

func (g *RoutineGroup) Cancel() {
	if g != nil {
		g.cancel()
	}
}

func (g *RoutineGroup) record(err error) {
	if err == nil {
		return
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	g.err = errors.Join(g.err, err)
}

func NewBulkExecutor(max int, interval time.Duration, handler func(context.Context, []any) error) *BulkExecutor {
	if max <= 0 {
		max = 1
	}
	return &BulkExecutor{
		max:      max,
		interval: interval,
		handler:  handler,
	}
}

func (e *BulkExecutor) Add(ctx context.Context, item any) error {
	if e == nil {
		return nil
	}
	e.mu.Lock()
	e.items = append(e.items, item)
	shouldFlush := len(e.items) >= e.max
	if !shouldFlush && e.interval > 0 && e.timer == nil {
		e.timer = time.AfterFunc(e.interval, func() {
			_ = e.Flush(context.Background())
		})
	}
	e.mu.Unlock()
	if shouldFlush {
		return e.Flush(ctx)
	}
	return nil
}

func (e *BulkExecutor) Flush(ctx context.Context) error {
	if e == nil {
		return nil
	}
	e.mu.Lock()
	items := append([]any(nil), e.items...)
	e.items = e.items[:0]
	if e.timer != nil {
		e.timer.Stop()
		e.timer = nil
	}
	e.mu.Unlock()
	if len(items) == 0 || e.handler == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	return e.handler(ctx, items)
}

func (e *BulkExecutor) Close(ctx context.Context) error {
	return e.Flush(ctx)
}

func (e PeriodicExecutor) Run(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	interval := e.Interval
	if interval <= 0 {
		interval = time.Second
	}
	if e.Immediate {
		if err := runPeriodicHandler(ctx, e.Handler); err != nil {
			return err
		}
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if err := runPeriodicHandler(ctx, e.Handler); err != nil {
				return err
			}
		}
	}
}

func Delay(ctx context.Context, delay time.Duration, fn func(context.Context) error) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if delay > 0 {
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
	return runPeriodicHandler(ctx, fn)
}

func runPeriodicHandler(ctx context.Context, fn func(context.Context) error) (err error) {
	if fn == nil {
		return nil
	}
	defer recoverPanic(&err)
	return fn(ctx)
}
