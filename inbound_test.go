package worker

import (
	"context"
	"testing"

	"github.com/nucleuskit/nucleus/core/inbound"
)

func TestNewInboundRequestFromWorkerMessagePreservesTopicPayloadAndMetadata(t *testing.T) {
	message := Message{
		ID:      "message-1",
		Topic:   "users.created",
		Payload: []byte(`{"id":"42"}`),
		Headers: map[string]string{
			"traceparent":  "trace-worker",
			"x-request-id": "request-worker",
			"x-tenant-id":  "tenant-worker",
		},
	}

	got := NewInboundRequest(context.Background(), message)

	if got.Kind != inbound.KindWorker {
		t.Fatalf("expected worker kind, got %q", got.Kind)
	}
	if got.Route.Method != "CONSUME" || got.Route.Path != "users.created" {
		t.Fatalf("unexpected route: %#v", got.Route)
	}
	if string(got.Body.Bytes) != `{"id":"42"}` {
		t.Fatalf("unexpected body bytes: %q", string(got.Body.Bytes))
	}
	if got.Metadata.Get("message_id") != "message-1" {
		t.Fatalf("expected message_id metadata, got %#v", got.Metadata)
	}
	if got.TraceID() != "trace-worker" || got.RequestID() != "request-worker" || got.Tenant() != "tenant-worker" {
		t.Fatalf("metadata did not propagate: %#v", got.Metadata)
	}
}
