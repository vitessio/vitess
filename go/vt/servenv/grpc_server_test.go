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

package servenv

import (
	"testing"

	"golang.org/x/net/context"

	"google.golang.org/grpc"
)

func TestEmpty(t *testing.T) {
	interceptors := &serverInterceptorBuilder{}
	if len(interceptors.Build()) > 0 {
		t.Fatalf("expected empty builder to report as empty")
	}
}

func TestSingleInterceptor(t *testing.T) {
	interceptors := &serverInterceptorBuilder{}
	fake := &FakeInterceptor{}

	interceptors.Add(fake.StreamServerInterceptor, fake.UnaryServerInterceptor)

	if len(interceptors.streamInterceptors) != 1 {
		t.Fatalf("expected 1 server options to be available")
	}
	if len(interceptors.unaryInterceptors) != 1 {
		t.Fatalf("expected 1 server options to be available")
	}
}

func TestDoubleInterceptor(t *testing.T) {
	interceptors := &serverInterceptorBuilder{}
	fake1 := &FakeInterceptor{name: "ettan"}
	fake2 := &FakeInterceptor{name: "tvaon"}

	interceptors.Add(fake1.StreamServerInterceptor, fake1.UnaryServerInterceptor)
	interceptors.Add(fake2.StreamServerInterceptor, fake2.UnaryServerInterceptor)

	if len(interceptors.streamInterceptors) != 2 {
		t.Fatalf("expected 1 server options to be available")
	}
	if len(interceptors.unaryInterceptors) != 2 {
		t.Fatalf("expected 1 server options to be available")
	}
}

type FakeInterceptor struct {
	name       string
	streamSeen interface{}
	unarySeen  interface{}
}

func (fake *FakeInterceptor) StreamServerInterceptor(value interface{}, stream grpc.ServerStream, _ *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	fake.streamSeen = value
	return handler(value, stream)
}

func (fake *FakeInterceptor) UnaryServerInterceptor(ctx context.Context, value interface{}, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp interface{}, err error) {
	fake.unarySeen = value
	return handler(ctx, value)
}
