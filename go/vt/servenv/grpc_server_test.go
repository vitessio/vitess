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

package servenv

import (
  "testing"

  "golang.org/x/net/context"

  "google.golang.org/grpc"
)

func TestEmpty(t *testing.T) {
  interceptors := &InterceptorBuilder{}
  if interceptors.NonEmpty() {
    t.Fatalf("expected empty builder to report as empty")
  }
}

func TestSingleInterceptor(t *testing.T) {
  interceptors := &InterceptorBuilder{}
  fake := &FakeInterceptor{}

  interceptors.Add(fake.StreamServerInterceptor, fake.UnaryServerInterceptor)

  if !interceptors.NonEmpty() {
    t.Fatalf("non-empty collector claims to have stuff")
  }

  _ = interceptors.StreamServerInterceptor(42, nil, nil, nullSHandler)
  _, _ = interceptors.UnaryStreamInterceptor(context.Background(), 666, nil, nullUHandler)

  assertEquals(t, fake.streamSeen, 42)
  assertEquals(t, fake.unarySeen, 666)
}

func TestDoubleInterceptor(t *testing.T) {
  interceptors := &InterceptorBuilder{}
  fake1 := &FakeInterceptor{name: "ettan"}
  fake2 := &FakeInterceptor{name: "tvaon"}

  interceptors.Add(fake1.StreamServerInterceptor, fake1.UnaryServerInterceptor)
  interceptors.Add(fake2.StreamServerInterceptor, fake2.UnaryServerInterceptor)

  if !interceptors.NonEmpty() {
    t.Fatalf("non-empty collector claims to have stuff")
  }

  _ = interceptors.StreamServerInterceptor(42, nil, nil, nullSHandler)
  _, _ = interceptors.UnaryStreamInterceptor(context.Background(), 666, nil, nullUHandler)

  assertEquals(t, fake1.streamSeen, 42)
  assertEquals(t, fake1.unarySeen, 666)
  assertEquals(t, fake2.streamSeen, 42)
  assertEquals(t, fake2.unarySeen, 666)
}

func nullSHandler(_ interface{}, _ grpc.ServerStream) error {
  return nil
}

func nullUHandler(_ context.Context, req interface{}) (interface{}, error) {
  return req, nil
}

func assertEquals(t *testing.T, a, b interface{}) {
  if a != b {
    t.Errorf("expected %v but got %v", a, b)
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
