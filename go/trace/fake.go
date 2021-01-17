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
	"io"

	"context"

	"google.golang.org/grpc"
)

type noopTracingServer struct{}

func (noopTracingServer) New(Span, string) Span { return NoopSpan{} }
func (noopTracingServer) NewClientSpan(parent Span, serviceName, label string) Span {
	return NoopSpan{}
}
func (noopTracingServer) FromContext(context.Context) (Span, bool)                  { return nil, false }
func (noopTracingServer) NewFromString(parent, label string) (Span, error)          { return NoopSpan{}, nil }
func (noopTracingServer) NewContext(parent context.Context, _ Span) context.Context { return parent }
func (noopTracingServer) AddGrpcServerOptions(addInterceptors func(s grpc.StreamServerInterceptor, u grpc.UnaryServerInterceptor)) {
}
func (noopTracingServer) AddGrpcClientOptions(addInterceptors func(s grpc.StreamClientInterceptor, u grpc.UnaryClientInterceptor)) {
}

// NoopSpan implements Span with no-op methods.
type NoopSpan struct{}

func (NoopSpan) Finish()                      {}
func (NoopSpan) Annotate(string, interface{}) {}

func init() {
	tracingBackendFactories["noop"] = func(_ string) (tracingService, io.Closer, error) {
		return noopTracingServer{}, &nilCloser{}, nil
	}
}
