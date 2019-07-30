/*
Copyright 2017 Google Inc.

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
	"github.com/opentracing-contrib/go-grpc"
	"github.com/opentracing/opentracing-go"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

var _ Span = (*openTracingSpan)(nil)

type openTracingSpan struct {
	otSpan opentracing.Span
}

// Finish will mark a span as finished
func (js openTracingSpan) Finish() {
	js.otSpan.Finish()
}

// Annotate will add information to an existing span
func (js openTracingSpan) Annotate(key string, value interface{}) {
	js.otSpan.SetTag(key, value)
}

var _ tracingService = (*openTracingService)(nil)

type openTracingService struct {
	Tracer opentracing.Tracer
}

// AddGrpcServerOptions is part of an interface implementation
func (jf openTracingService) AddGrpcServerOptions(addInterceptors func(s grpc.StreamServerInterceptor, u grpc.UnaryServerInterceptor)) {
	addInterceptors(otgrpc.OpenTracingStreamServerInterceptor(jf.Tracer), otgrpc.OpenTracingServerInterceptor(jf.Tracer))
}

// AddGrpcClientOptions is part of an interface implementation
func (jf openTracingService) AddGrpcClientOptions(addInterceptors func(s grpc.StreamClientInterceptor, u grpc.UnaryClientInterceptor)) {
	addInterceptors(otgrpc.OpenTracingStreamClientInterceptor(jf.Tracer), otgrpc.OpenTracingClientInterceptor(jf.Tracer))
}

// NewClientSpan is part of an interface implementation
func (jf openTracingService) NewClientSpan(parent Span, serviceName, label string) Span {
	span := jf.New(parent, label)
	span.Annotate("peer.service", serviceName)
	return span
}

// New is part of an interface implementation
func (jf openTracingService) New(parent Span, label string) Span {
	var innerSpan opentracing.Span
	if parent == nil {
		innerSpan = jf.Tracer.StartSpan(label)
	} else {
		jaegerParent := parent.(openTracingSpan)
		span := jaegerParent.otSpan
		innerSpan = jf.Tracer.StartSpan(label, opentracing.ChildOf(span.Context()))
	}
	return openTracingSpan{otSpan: innerSpan}
}

// FromContext is part of an interface implementation
func (jf openTracingService) FromContext(ctx context.Context) (Span, bool) {
	innerSpan := opentracing.SpanFromContext(ctx)

	if innerSpan != nil {
		return openTracingSpan{otSpan: innerSpan}, true
	} else {
		return nil, false
	}
}

// NewContext is part of an interface implementation
func (jf openTracingService) NewContext(parent context.Context, s Span) context.Context {
	span, ok := s.(openTracingSpan)
	if !ok {
		return nil
	}
	return opentracing.ContextWithSpan(parent, span.otSpan)
}
