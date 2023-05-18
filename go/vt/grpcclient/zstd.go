/*
Copyright 2023 The Vitess Authors.

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

package grpcclient

import (
	"io"

	"github.com/klauspost/compress/zstd"
)

// ZstdCompressor is a gRPC compressor using zstd
type ZstdCompressor struct{}

// Name is "zstd"
func (s ZstdCompressor) Name() string {
	return "zstd"
}

// Compress wraps with a ZstdReader
func (s ZstdCompressor) Compress(w io.Writer) (io.WriteCloser, error) {
	enc, err := zstd.NewWriter(w)
	if err != nil {
		return nil, err
	}
	return enc, nil
}

// Decompress wraps with a ZstdReader
func (s ZstdCompressor) Decompress(r io.Reader) (io.Reader, error) {
	dec, err := zstd.NewReader(r)
	if err != nil {
		return nil, err
	}
	return dec, nil
}
