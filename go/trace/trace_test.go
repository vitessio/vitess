/*
Copyright 2019 The Vitess Authors.

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
	"fmt"
	"io"
	"testing"

	"github.com/opentracing/opentracing-go"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"vitess.io/vitess/go/viperutil/vipertest"
)

func TestFakeSpan(t *testing.T) {
	ctx := context.Background()

	// It should be safe to call all the usual methods as if a plugin were installed.
	span1, ctx := NewSpan(ctx, "label")
	span1.Finish()

	span2, ctx := NewSpan(ctx, "label")
	span2.Annotate("key", 42)
	span2.Finish()

	span3, _ := NewSpan(ctx, "label")
	span3.Annotate("key", 42)
	span3.Finish()
}

func TestRegisterService(t *testing.T) {
	fakeName := "test"
	tracingBackendFactories[fakeName] = func(serviceName string) (tracingService, io.Closer, error) {
		tracer := &fakeTracer{name: serviceName}
		return tracer, tracer, nil
	}

	v := viper.New()
	t.Cleanup(vipertest.Stub(t, v, tracingServer))

	v.Set(tracingServer.Key(), fakeName)

	serviceName := "vtservice"
	closer := StartTracing(serviceName)
	tracer, ok := closer.(*fakeTracer)
	if !ok {
		t.Fatalf("did not get the expected tracer, got %+v (%T)", tracer, tracer)
	}

	if tracer.name != serviceName {
		t.Fatalf("expected the name to be `%v` but it was `%v`", serviceName, tracer.name)
	}
}

func TestNewFromString(t *testing.T) {
	tests := []struct {
		parent      string
		label       string
		context     context.Context
		expectedLog string
		isPresent   bool
		expectedErr string
	}{
		{
			parent:      "",
			label:       "empty parent",
			context:     context.TODO(),
			expectedLog: "",
			isPresent:   true,
			expectedErr: "parent is empty",
		},
		{
			parent:      "parent",
			label:       "non-empty parent",
			expectedLog: "[key: sql-statement-type values:non-empty parent]\n",
			context:     context.Background(),
			isPresent:   false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.label, func(t *testing.T) {
			span, ctx, err := NewFromString(context.Background(), tt.parent, tt.label)
			if tt.expectedErr == "" {
				require.NoError(t, err)
				require.NotEmpty(t, span)
				require.Equal(t, tt.context, ctx)

				got := captureOutput(t, func() {
					AnnotateSQL(span, &fakeStringer{tt.label})
				}, true)

				require.Equal(t, tt.expectedLog, got)
			} else {
				require.ErrorContains(t, err, tt.expectedErr)
				require.Nil(t, span)
				require.Nil(t, ctx)
			}

			copySpan := CopySpan(context.TODO(), tt.context)
			if tt.isPresent {
				require.Equal(t, tt.context, copySpan)
			} else {
				require.Equal(t, context.TODO(), copySpan)
			}
		})
	}
}

func TestNilCloser(t *testing.T) {
	nc := nilCloser{}
	require.Nil(t, nc.Close())
}

type fakeTracer struct {
	name string
	log  []string
}

func (f *fakeTracer) GetOpenTracingTracer() opentracing.Tracer {
	return opentracing.GlobalTracer()
}

func (f *fakeTracer) NewFromString(parent, label string) (Span, error) {
	if parent == "" {
		return &mockSpan{tracer: f}, fmt.Errorf("parent is empty")
	}
	return &mockSpan{tracer: f}, nil
}

func (f *fakeTracer) New(parent Span, label string) Span {
	f.log = append(f.log, "span started")

	return &mockSpan{tracer: f}
}

func (f *fakeTracer) FromContext(ctx context.Context) (Span, bool) {
	if ctx == context.Background() {
		return nil, false
	}
	return &mockSpan{}, true
}

func (f *fakeTracer) NewContext(parent context.Context, span Span) context.Context {
	return parent
}

func (f *fakeTracer) AddGrpcServerOptions(addInterceptors func(s grpc.StreamServerInterceptor, u grpc.UnaryServerInterceptor)) {
	panic("implement me")
}

func (f *fakeTracer) AddGrpcClientOptions(addInterceptors func(s grpc.StreamClientInterceptor, u grpc.UnaryClientInterceptor)) {
	panic("implement me")
}

func (f *fakeTracer) Close() error {
	panic("implement me")
}

type mockSpan struct {
	tracer *fakeTracer
}

func (m *mockSpan) Finish() {
	m.tracer.log = append(m.tracer.log, "span finished")
}

func (m *mockSpan) Annotate(key string, value any) {
	m.tracer.log = append(m.tracer.log, fmt.Sprintf("key: %v values:%v", key, value))
	fmt.Println(m.tracer.log)
}

type fakeStringer struct {
	str string
}

func (fs *fakeStringer) String() string {
	return fs.str
}
