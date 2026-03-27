/*
Copyright 2026 The Vitess Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package trace

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"sync"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/propagation"
	oteltrace "go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
)

var _ Span = (*otelSpan)(nil)

type otelSpan struct {
	span oteltrace.Span
}

func (s *otelSpan) Finish() {
	s.span.End()
}

func (s *otelSpan) Annotate(key string, value any) {
	s.span.SetAttributes(keyValue(key, value))
}

var _ tracingService = (*otelTracingService)(nil)

type otelTracingService struct {
	Tracer oteltrace.Tracer
}

func (ots *otelTracingService) New(ctx context.Context, label string) (Span, context.Context) {
	ctx, span := ots.Tracer.Start(ctx, label)
	return &otelSpan{span: span}, ctx
}

func (ots *otelTracingService) NewFromString(ctx context.Context, parent, label string) (Span, context.Context, error) {
	carrier, err := extractCarrierFromString(parent)
	if err != nil {
		return nil, nil, vterrors.Wrapf(err, "failed to decode span carrier")
	}

	propagator := otel.GetTextMapPropagator()
	extractedCtx := propagator.Extract(ctx, carrier)

	spanCtx := oteltrace.SpanContextFromContext(extractedCtx)
	if !spanCtx.IsValid() {
		return nil, nil, vterrors.New(vtrpcpb.Code_INVALID_ARGUMENT, "extracted span context is not valid")
	}

	extractedCtx, span := ots.Tracer.Start(extractedCtx, label)
	return &otelSpan{span: span}, extractedCtx, nil
}

func (ots *otelTracingService) FromContext(ctx context.Context) (Span, bool) {
	span := oteltrace.SpanFromContext(ctx)
	if !span.SpanContext().IsValid() {
		return nil, false
	}
	return &otelSpan{span: span}, true
}

func (ots *otelTracingService) NewContext(parent context.Context, s Span) context.Context {
	oSpan, ok := s.(*otelSpan)
	if !ok {
		return parent
	}
	return oteltrace.ContextWithSpan(parent, oSpan.span)
}

func (ots *otelTracingService) AddGrpcServerOptions(addInterceptors func(s grpc.StreamServerInterceptor, u grpc.UnaryServerInterceptor)) {
	addInterceptors(
		ots.otelStreamServerInterceptor(),
		ots.otelUnaryServerInterceptor(),
	)
}

func (ots *otelTracingService) AddGrpcClientOptions(addInterceptors func(s grpc.StreamClientInterceptor, u grpc.UnaryClientInterceptor)) {
	addInterceptors(
		ots.otelStreamClientInterceptor(),
		ots.otelUnaryClientInterceptor(),
	)
}

func (ots *otelTracingService) otelUnaryServerInterceptor() grpc.UnaryServerInterceptor {
	propagator := otel.GetTextMapPropagator()
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		ctx = extractFromGRPCMetadata(ctx, propagator)
		ctx, span := ots.Tracer.Start(ctx, info.FullMethod, oteltrace.WithSpanKind(oteltrace.SpanKindServer))
		defer span.End()
		resp, err := handler(ctx, req)
		if err != nil {
			span.RecordError(err)
		}
		return resp, err
	}
}

func (ots *otelTracingService) otelStreamServerInterceptor() grpc.StreamServerInterceptor {
	propagator := otel.GetTextMapPropagator()
	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		ctx := extractFromGRPCMetadata(ss.Context(), propagator)
		ctx, span := ots.Tracer.Start(ctx, info.FullMethod, oteltrace.WithSpanKind(oteltrace.SpanKindServer))
		defer span.End()
		err := handler(srv, &wrappedServerStream{ServerStream: ss, ctx: ctx})
		if err != nil {
			span.RecordError(err)
		}
		return err
	}
}

func (ots *otelTracingService) otelUnaryClientInterceptor() grpc.UnaryClientInterceptor {
	propagator := otel.GetTextMapPropagator()
	return func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		ctx, span := ots.Tracer.Start(ctx, method, oteltrace.WithSpanKind(oteltrace.SpanKindClient))
		defer span.End()
		ctx = injectIntoGRPCMetadata(ctx, propagator)
		err := invoker(ctx, method, req, reply, cc, opts...)
		if err != nil {
			span.RecordError(err)
		}
		return err
	}
}

func (ots *otelTracingService) otelStreamClientInterceptor() grpc.StreamClientInterceptor {
	propagator := otel.GetTextMapPropagator()
	return func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
		ctx, span := ots.Tracer.Start(ctx, method, oteltrace.WithSpanKind(oteltrace.SpanKindClient))
		ctx = injectIntoGRPCMetadata(ctx, propagator)
		cs, err := streamer(ctx, desc, cc, method, opts...)
		if err != nil {
			span.RecordError(err)
			span.End()
			return cs, err
		}
		return &trackedClientStream{ClientStream: cs, span: span}, nil
	}
}

// trackedClientStream wraps grpc.ClientStream to end the span when the stream
// finishes. The span is ended exactly once via sync.Once when RecvMsg returns
// an error (including io.EOF) or when SendMsg/CloseSend fails.
type trackedClientStream struct {
	grpc.ClientStream
	span    oteltrace.Span
	endOnce sync.Once
}

func (s *trackedClientStream) endSpan() {
	s.endOnce.Do(func() {
		s.span.End()
	})
}

func (s *trackedClientStream) CloseSend() error {
	err := s.ClientStream.CloseSend()
	if err != nil {
		s.span.RecordError(err)
		s.endSpan()
	}
	return err
}

func (s *trackedClientStream) RecvMsg(m any) error {
	err := s.ClientStream.RecvMsg(m)
	if err != nil {
		if err != io.EOF {
			s.span.RecordError(err)
		}
		s.endSpan()
	}
	return err
}

func (s *trackedClientStream) SendMsg(m any) error {
	err := s.ClientStream.SendMsg(m)
	if err != nil {
		s.span.RecordError(err)
		s.endSpan()
	}
	return err
}

// extractFromGRPCMetadata extracts trace context from incoming gRPC metadata.
func extractFromGRPCMetadata(ctx context.Context, propagator propagation.TextMapPropagator) context.Context {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return ctx
	}
	return propagator.Extract(ctx, &metadataCarrier{md: md})
}

// injectIntoGRPCMetadata injects trace context into outgoing gRPC metadata.
// The metadata is copied before injection to avoid mutating shared state.
func injectIntoGRPCMetadata(ctx context.Context, propagator propagation.TextMapPropagator) context.Context {
	md, ok := metadata.FromOutgoingContext(ctx)
	if !ok {
		md = metadata.MD{}
	}
	md = md.Copy()
	propagator.Inject(ctx, &metadataCarrier{md: md})
	return metadata.NewOutgoingContext(ctx, md)
}

// metadataCarrier adapts gRPC metadata.MD to propagation.TextMapCarrier.
type metadataCarrier struct {
	md metadata.MD
}

func (c *metadataCarrier) Get(key string) string {
	vals := c.md.Get(key)
	if len(vals) > 0 {
		return vals[0]
	}
	return ""
}

func (c *metadataCarrier) Set(key, val string) {
	c.md.Set(key, val)
}

func (c *metadataCarrier) Keys() []string {
	keys := make([]string, 0, len(c.md))
	for k := range c.md {
		keys = append(keys, k)
	}
	return keys
}

// wrappedServerStream wraps grpc.ServerStream to override the context.
type wrappedServerStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (w *wrappedServerStream) Context() context.Context {
	return w.ctx
}

// extractCarrierFromString decodes a base64-encoded JSON map into a propagation.MapCarrier.
func extractCarrierFromString(in string) (propagation.MapCarrier, error) {
	decodedBytes, err := base64.StdEncoding.DecodeString(in)
	if err != nil {
		return nil, err
	}

	var dat map[string]string
	err = json.Unmarshal(decodedBytes, &dat)
	if err != nil {
		return nil, err
	}

	return propagation.MapCarrier(dat), nil
}

// keyValue converts a key and any-typed value into an OTel attribute.KeyValue.
func keyValue(key string, value any) attribute.KeyValue {
	switch v := value.(type) {
	case string:
		return attribute.String(key, v)
	case bool:
		return attribute.Bool(key, v)
	case int:
		return attribute.Int(key, v)
	case int64:
		return attribute.Int64(key, v)
	case float64:
		return attribute.Float64(key, v)
	default:
		return attribute.String(key, fmt.Sprint(v))
	}
}
