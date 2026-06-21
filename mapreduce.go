package worker

import (
	"context"
	"errors"
	"sync"
)

var ErrMapReduceFuncRequired = errors.New("worker: map and reduce functions are required")

type mapReduceJob[T any] struct {
	value T
}

type mapReduceResult[M any] struct {
	value M
	err   error
}

func MapReduce[T any, M any, R any](
	ctx context.Context,
	inputs []T,
	workers int,
	mapFn func(context.Context, T) (M, error),
	reduceFn func(context.Context, R, M) (R, error),
	zero R,
) (R, error) {
	if mapFn == nil || reduceFn == nil {
		return zero, ErrMapReduceFuncRequired
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if len(inputs) == 0 {
		return zero, ctx.Err()
	}
	if workers <= 0 {
		workers = 1
	}
	if workers > len(inputs) {
		workers = len(inputs)
	}

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	jobs := make(chan mapReduceJob[T], len(inputs))
	results := make(chan mapReduceResult[M], len(inputs))
	for _, input := range inputs {
		jobs <- mapReduceJob[T]{value: input}
	}
	close(jobs)

	var wg sync.WaitGroup
	wg.Add(workers)
	for i := 0; i < workers; i++ {
		go func() {
			defer wg.Done()
			for job := range jobs {
				if err := runCtx.Err(); err != nil {
					return
				}
				value, err := mapFn(runCtx, job.value)
				if err != nil {
					results <- mapReduceResult[M]{err: err}
					cancel()
					return
				}
				select {
				case results <- mapReduceResult[M]{value: value}:
				case <-runCtx.Done():
					return
				}
			}
		}()
	}
	go func() {
		wg.Wait()
		close(results)
	}()

	acc := zero
	var firstErr error
	for result := range results {
		if result.err != nil {
			if firstErr == nil {
				firstErr = result.err
				cancel()
			}
			continue
		}
		if firstErr != nil {
			continue
		}
		next, err := reduceFn(runCtx, acc, result.value)
		if err != nil {
			firstErr = err
			cancel()
			continue
		}
		acc = next
	}
	if firstErr != nil {
		return zero, firstErr
	}
	if err := ctx.Err(); err != nil {
		return zero, err
	}
	return acc, nil
}
