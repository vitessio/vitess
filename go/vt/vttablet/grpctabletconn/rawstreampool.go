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

package grpctabletconn

import (
	"context"
	"errors"
	"sync"
)

var ErrPoolClosed = errors.New("stream pool is closed")

const defaultMaxIdleStreams = 2

type pooledStream[T any] struct {
	stream T
	cancel context.CancelFunc
}

type streamPool[T any] struct {
	mu      sync.Mutex
	streams []*pooledStream[T]
	create  func() (T, context.CancelFunc, error)
	closed  bool
	maxIdle int
}

func newStreamPool[T any](maxIdle int, create func() (T, context.CancelFunc, error)) *streamPool[T] {
	return &streamPool[T]{
		create:  create,
		maxIdle: maxIdle,
	}
}

func (p *streamPool[T]) get() (*pooledStream[T], error) {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		var zero T
		return &pooledStream[T]{stream: zero}, ErrPoolClosed
	}
	if n := len(p.streams); n > 0 {
		s := p.streams[n-1]
		p.streams = p.streams[:n-1]
		p.mu.Unlock()
		return s, nil
	}
	p.mu.Unlock()

	stream, cancel, err := p.create()
	if err != nil {
		return nil, err
	}
	return &pooledStream[T]{stream: stream, cancel: cancel}, nil
}

func (p *streamPool[T]) put(s *pooledStream[T]) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed || len(p.streams) >= p.maxIdle {
		s.cancel()
		return
	}
	p.streams = append(p.streams, s)
}

func (p *streamPool[T]) discard(s *pooledStream[T]) {
	s.cancel()
}

func (p *streamPool[T]) close() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.closed = true
	for _, s := range p.streams {
		s.cancel()
	}
	p.streams = nil
}
