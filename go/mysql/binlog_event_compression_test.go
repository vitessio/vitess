/*
Copyright 2024 The Vitess Authors.

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

package mysql

import (
	"bytes"
	"io"
	"testing"

	"github.com/klauspost/compress/zstd"
	"github.com/stretchr/testify/require"
)

func TestDecoderPool(t *testing.T) {
	validateDecoder := func(t *testing.T, err error, decoder *zstd.Decoder) {
		require.NoError(t, err)
		require.NotNil(t, decoder)
		require.IsType(t, &zstd.Decoder{}, decoder)
	}
	tests := []struct {
		name    string
		reader  io.Reader
		wantErr bool
	}{
		{
			name:   "happy path",
			reader: bytes.NewReader([]byte{0x68, 0x61, 0x70, 0x70, 0x79}),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// It's not guaranteed that we get the same decoder back from the pool
			// that we just put in, so we use a loop and ensure that it worked at
			// least one of the times. Without doing this the test would be flaky.
			poolingUsed := false

			for range 20 {
				decoder, err := statefulDecoderPool.Get(tt.reader)
				validateDecoder(t, err, decoder)
				statefulDecoderPool.Put(decoder)

				decoder2, err := statefulDecoderPool.Get(tt.reader)
				validateDecoder(t, err, decoder2)
				if decoder2 == decoder {
					poolingUsed = true
				}
				statefulDecoderPool.Put(decoder2)

				decoder3, err := statefulDecoderPool.Get(tt.reader)
				validateDecoder(t, err, decoder3)
				if decoder3 == decoder || decoder3 == decoder2 {
					poolingUsed = true
				}
				statefulDecoderPool.Put(decoder3)
			}

			require.True(t, poolingUsed)
		})
	}
}
