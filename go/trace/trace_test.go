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

	"github.com/opentracing/opentracing-go"
	"golang.org/x/net/context"
)

func TestFakeSpan(t *testing.T) {
	ctx := context.Background()
	RegisterSpanFactory(fakeSpanFactory{})

	// It should be safe to call all the usual methods as if a plugin were installed.
	span1, ctx := NewSpan(ctx, "label", Local)
	span1.Finish()

	span2, ctx := NewSpan(ctx, "label", Client)
	span2.Annotate("key", 42)
	span2.Finish()

	span3, ctx := NewSpan(ctx, "label", Server)
	span3.Annotate("key", 42)
	span3.Finish()
}

func TestRegisterService(t *testing.T) {
	fakeName := "test"
	tracingBackendFactories[fakeName] = func(s string) (opentracing.Tracer, io.Closer, error) {
		tracer := fakeTracer{name: s}
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

func (fakeTracer) Close() error {
	panic("implement me")
}

func (fakeTracer) StartSpan(operationName string, opts ...opentracing.StartSpanOption) opentracing.Span {
	panic("implement me")
}

func (fakeTracer) Inject(sm opentracing.SpanContext, format interface{}, carrier interface{}) error {
	panic("implement me")
}

func (fakeTracer) Extract(format interface{}, carrier interface{}) (opentracing.SpanContext, error) {
	panic("implement me")
}
