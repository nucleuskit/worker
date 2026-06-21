package worker

import "context"

type Stream[T any] func(context.Context, func(T) error) error

func SliceStream[T any](items []T) Stream[T] {
	return func(ctx context.Context, yield func(T) error) error {
		if ctx == nil {
			ctx = context.Background()
		}
		for _, item := range items {
			if err := ctx.Err(); err != nil {
				return err
			}
			if err := yield(item); err != nil {
				return err
			}
		}
		return nil
	}
}

func MapStream[T any, U any](source Stream[T], fn func(context.Context, T) (U, error)) Stream[U] {
	return func(ctx context.Context, yield func(U) error) error {
		if source == nil || fn == nil {
			return nil
		}
		if ctx == nil {
			ctx = context.Background()
		}
		return source(ctx, func(item T) error {
			mapped, err := fn(ctx, item)
			if err != nil {
				return err
			}
			return yield(mapped)
		})
	}
}

func FilterStream[T any](source Stream[T], fn func(context.Context, T) (bool, error)) Stream[T] {
	return func(ctx context.Context, yield func(T) error) error {
		if source == nil || fn == nil {
			return nil
		}
		if ctx == nil {
			ctx = context.Background()
		}
		return source(ctx, func(item T) error {
			keep, err := fn(ctx, item)
			if err != nil {
				return err
			}
			if !keep {
				return nil
			}
			return yield(item)
		})
	}
}

func ForEachStream[T any](ctx context.Context, source Stream[T], fn func(context.Context, T) error) error {
	if source == nil || fn == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	return source(ctx, func(item T) error {
		return fn(ctx, item)
	})
}
