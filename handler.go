package worker

import (
	"context"

	"github.com/nucleuskit/nucleus/cap/mq"
)

type Message struct {
	ID       string
	Topic    string
	Key      string
	Payload  []byte
	Headers  map[string]string
	Metadata mq.Metadata
}

type Handler interface {
	Handle(context.Context, Message) error
}

type HandlerFunc func(context.Context, Message) error

func (fn HandlerFunc) Handle(ctx context.Context, message Message) error {
	return fn(ctx, message)
}
