package worker

import (
	"context"

	nucleuscontext "github.com/nucleuskit/core/context"
	"github.com/nucleuskit/core/inbound"
)

func NewInboundRequest(ctx context.Context, message Message) inbound.Request {
	metadata := metadataFromMessage(ctx, message)
	return inbound.Request{
		Kind: inbound.KindWorker,
		Route: inbound.Route{
			Method: "CONSUME",
			Path:   message.Topic,
		},
		Body: inbound.Body{
			Bytes: append([]byte(nil), message.Payload...),
		},
		Metadata: metadata,
	}
}

func metadataFromMessage(ctx context.Context, message Message) inbound.Metadata {
	metadata := inbound.Metadata{}
	for key, value := range message.Headers {
		metadata.Set(key, value)
	}
	setIfPresent(metadata, "message_id", message.ID)
	setIfPresent(metadata, inbound.KeyTraceID, metadata.Get(inbound.HeaderTraceParent))
	setIfPresent(metadata, inbound.KeyRequestID, metadata.Get(inbound.HeaderRequestID))
	setIfPresent(metadata, inbound.KeyTenant, metadata.Get(inbound.HeaderTenant))
	if metadata.Get(inbound.KeyTraceID) == "" {
		setIfPresent(metadata, inbound.KeyTraceID, nucleuscontext.TraceID(ctx))
	}
	if metadata.Get(inbound.KeyRequestID) == "" {
		setIfPresent(metadata, inbound.KeyRequestID, nucleuscontext.RequestID(ctx))
	}
	if metadata.Get(inbound.KeyTenant) == "" {
		setIfPresent(metadata, inbound.KeyTenant, nucleuscontext.Tenant(ctx))
	}
	return metadata
}

func setIfPresent(metadata inbound.Metadata, key string, value string) {
	if value != "" {
		metadata.Set(key, value)
	}
}
