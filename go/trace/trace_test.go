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
	"io"
	"testing"

	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

func TestFakeSpan(t *testing.T) {
	ctx := context.Background()
	//RegisterSpanFactory(fakeSpanFactory{})

	// It should be safe to call all the usual methods as if a plugin were installed.
	span1, ctx := NewSpan(ctx, "label")
	span1.Finish()

	span2, ctx := NewSpan(ctx, "label")
	span2.Annotate("key", 42)
	span2.Finish()

	span3, ctx := NewSpan(ctx, "label")
	span3.Annotate("key", 42)
	span3.Finish()
}

func TestRegisterService(t *testing.T) {
	fakeName := "test"
	tracingBackendFactories[fakeName] = func(serviceName string) (TracingService, io.Closer, error) {
		tracer := fakeTracer{name: serviceName}
		return tracer, tracer, nil
	}

	tracingServer = &fakeName

	serviceName := "vtservice"
	closer := StartTracing(serviceName)
	tracer, ok := closer.(fakeTracer)
	if !ok {
		t.Fatalf("did not get the expected tracer")
	}

	if tracer.name != serviceName {
		t.Fatalf("expected the name to be `%v` but it was `%v`", serviceName, tracer.name)
	}
}

type fakeTracer struct {
	name string
}

func (fakeTracer) NewClientSpan(parent Span, serviceName, label string) Span {
	panic("implement me")
}

func (fakeTracer) New(parent Span, label string) Span {
	panic("implement me")
}

func (fakeTracer) FromContext(ctx context.Context) (Span, bool) {
	panic("implement me")
}

func (fakeTracer) NewContext(parent context.Context, span Span) context.Context {
	panic("implement me")
}

func (fakeTracer) AddGrpcServerOptions(addInterceptors func(s grpc.StreamServerInterceptor, u grpc.UnaryServerInterceptor)) {
	panic("implement me")
}

func (fakeTracer) GetGrpcServerOptions() []grpc.ServerOption {
	panic("implement me")
}

func (fakeTracer) GetGrpcClientOptions() []grpc.DialOption {
	panic("implement me")
}

func (fakeTracer) Close() error {
	panic("implement me")
}
