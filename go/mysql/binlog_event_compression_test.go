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
	type args struct {
		r io.Reader
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
			decoder, err := statefulDecoderPool.Get(tt.reader)
			require.NoError(t, err)
			require.NotNil(t, decoder)
			require.IsType(t, &zstd.Decoder{}, decoder)
			statefulDecoderPool.Put(decoder)
			decoder2, err := statefulDecoderPool.Get(tt.reader)
			require.NoError(t, err)
			require.NotNil(t, decoder2)
			require.IsType(t, &zstd.Decoder{}, decoder)
			statefulDecoderPool.Put(decoder)
			require.True(t, (decoder2 == decoder))
			statefulDecoderPool.Put(decoder2)
			decoder3, err := statefulDecoderPool.Get(tt.reader)
			require.NoError(t, err)
			require.IsType(t, &zstd.Decoder{}, decoder)
			statefulDecoderPool.Put(decoder)
			require.True(t, (decoder3 == decoder2))
		})
	}
}
