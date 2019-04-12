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
	"fmt"
	"io"
	"strings"
	"testing"

	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

func TestFakeSpan(t *testing.T) {
	ctx := context.Background()

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
		tracer := &fakeTracer{name: serviceName}
		return tracer, tracer, nil
	}

	tracingServer = &fakeName

	serviceName := "vtservice"
	closer := StartTracing(serviceName)
	tracer, ok := closer.(*fakeTracer)
	if !ok {
		t.Fatalf("did not get the expected tracer")
	}

	if tracer.name != serviceName {
		t.Fatalf("expected the name to be `%v` but it was `%v`", serviceName, tracer.name)
	}
}

func TestProtectPII(t *testing.T) {
	// set up fake tracer that we can assert on
	fakeName := "test"
	var tracer *fakeTracer
	tracingBackendFactories[fakeName] = func(serviceName string) (TracingService, io.Closer, error) {
		tracer = &fakeTracer{name: serviceName}
		return tracer, tracer, nil
	}

	tracingServer = &fakeName

	serviceName := "vtservice"
	closer := StartTracing(serviceName)
	_, ok := closer.(*fakeTracer)
	if !ok {
		t.Fatalf("did not get the expected tracer")
	}

	span, _ := NewSpan(context.Background(), "span-name")
	AnnotateSQL(span, "SELECT * FROM Tabble WHERE name = 'SECRET_INFORMATION'")
	span.Finish()

	tracer.assertNoSpanWith(t, "SECRET_INFORMATION")
}

type fakeTracer struct {
	name string
	log  []string
}

func (f *fakeTracer) NewClientSpan(parent Span, serviceName, label string) Span {
	panic("implement me")
}

func (f *fakeTracer) New(parent Span, label string) Span {
	f.log = append(f.log, "span started")

	return &mockSpan{tracer: f}
}

func (f *fakeTracer) FromContext(ctx context.Context) (Span, bool) {
	return nil, false
}

func (f *fakeTracer) NewContext(parent context.Context, span Span) context.Context {
	return parent
}

func (f *fakeTracer) AddGrpcServerOptions(addInterceptors func(s grpc.StreamServerInterceptor, u grpc.UnaryServerInterceptor)) {
	panic("implement me")
}

func (f *fakeTracer) GetGrpcServerOptions() []grpc.ServerOption {
	panic("implement me")
}

func (f *fakeTracer) GetGrpcClientOptions() []grpc.DialOption {
	panic("implement me")
}

func (f *fakeTracer) Close() error {
	panic("implement me")
}

func (f *fakeTracer) assertNoSpanWith(t *testing.T, substr string) {
	t.Helper()
	for _, logLine := range f.log {
		if strings.Contains(logLine, substr) {
			t.Fatalf("expected to not find [%v] but found it in [%v]", substr, logLine)
		}
	}
}

type mockSpan struct {
	tracer *fakeTracer
}

func (m *mockSpan) Finish() {
	m.tracer.log = append(m.tracer.log, "span finished")
}

func (m *mockSpan) Annotate(key string, value interface{}) {
	m.tracer.log = append(m.tracer.log, fmt.Sprintf("key: %v values:%v", key, value))
}
