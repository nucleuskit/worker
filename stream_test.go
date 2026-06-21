package worker

import (
	"context"
	"reflect"
	"testing"
)

func TestStreamMapFilterForEach(t *testing.T) {
	source := SliceStream([]int{1, 2, 3, 4})
	mapped := MapStream(source, func(ctx context.Context, value int) (int, error) {
		return value * 2, nil
	})
	filtered := FilterStream(mapped, func(ctx context.Context, value int) (bool, error) {
		return value > 4, nil
	})

	var got []int
	if err := ForEachStream(context.Background(), filtered, func(ctx context.Context, value int) error {
		got = append(got, value)
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	if want := []int{6, 8}; !reflect.DeepEqual(got, want) {
		t.Fatalf("expected %v, got %v", want, got)
	}
}
