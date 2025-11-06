/*
Copyright 2021 The Vitess Authors.

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
	"google.golang.org/grpc/encoding"
	"google.golang.org/grpc/mem"

	// Guarantee that the built-in proto is called registered before this one
	// so that it can be replaced.
	_ "google.golang.org/grpc/encoding/proto"
)

// Name is the name registered for the proto compressor.
const Name = "proto"

type vtprotoMessage interface {
	MarshalToSizedBufferVT(data []byte) (int, error)
	UnmarshalVT([]byte) error
	SizeVT() int
}

type Codec struct {
	fallback encoding.CodecV2
}

func (Codec) Name() string { return Name }

var defaultBufferPool = mem.DefaultBufferPool()

func (c *Codec) Marshal(v any) (mem.BufferSlice, error) {
	if m, ok := v.(vtprotoMessage); ok {
		size := m.SizeVT()
		if mem.IsBelowBufferPoolingThreshold(size) {
			buf := make([]byte, size)
			if _, err := m.MarshalToSizedBufferVT(buf[:size]); err != nil {
				return nil, err
			}
			return mem.BufferSlice{mem.SliceBuffer(buf)}, nil
		}
		buf := defaultBufferPool.Get(size)
		if _, err := m.MarshalToSizedBufferVT((*buf)[:size]); err != nil {
			defaultBufferPool.Put(buf)
			return nil, err
		}
		return mem.BufferSlice{mem.NewBuffer(buf, defaultBufferPool)}, nil
	}

	return c.fallback.Marshal(v)
}

func (c *Codec) Unmarshal(data mem.BufferSlice, v any) error {
	if m, ok := v.(vtprotoMessage); ok {
		buf := data.MaterializeToBuffer(defaultBufferPool)
		defer buf.Free()
		return m.UnmarshalVT(buf.ReadOnlyData())
	}

	return c.fallback.Unmarshal(data, v)
}

func init() {
	encoding.RegisterCodecV2(&Codec{
		fallback: encoding.GetCodecV2("proto"),
	})
}
