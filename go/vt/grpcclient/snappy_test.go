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

package grpcclient

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

func TestCompressDecompress(t *testing.T) {
	snappComp := SnappyCompressor{}
	writer, err := snappComp.Compress(&bytes.Buffer{})
	require.NoError(t, err)
	require.NotEmpty(t, writer)

	reader, err := snappComp.Decompress(&bytes.Buffer{})
	require.NoError(t, err)
	require.NotEmpty(t, reader)
}

func TestAppendCompression(t *testing.T) {
	oldCompression := compression
	defer func() {
		compression = oldCompression
	}()

	dialOpts := []grpc.DialOption{}
	dialOpts, err := appendCompression(dialOpts)
	require.NoError(t, err)
	require.Equal(t, 0, len(dialOpts))

	// Change the compression to snappy
	compression = "snappy"

	dialOpts, err = appendCompression(dialOpts)
	require.NoError(t, err)
	require.Equal(t, 1, len(dialOpts))

	// Change the compression to some unknown value
	compression = "unknown"

	dialOpts, err = appendCompression(dialOpts)
	require.NoError(t, err)
	require.Equal(t, 1, len(dialOpts))
}
