/*
Copyright 2025 The Vitess Authors.

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

package sortio

import (
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTempFileCreateAndUnlink(t *testing.T) {
	tf, err := NewTempFile("", 4096)
	require.NoError(t, err)
	defer tf.Close()

	// File should be unlinked, but fd should still work
	_, err = tf.Write([]byte("hello"))
	require.NoError(t, err)
}

func TestTempFileWriteAndRead(t *testing.T) {
	tf, err := NewTempFile("", 4096)
	require.NoError(t, err)
	defer tf.Close()

	data1 := []byte("first chunk")
	off1, err := tf.Write(data1)
	require.NoError(t, err)
	assert.Equal(t, int64(0), off1)

	data2 := []byte("second chunk")
	off2, err := tf.Write(data2)
	require.NoError(t, err)
	assert.Equal(t, int64(len(data1)), off2)

	require.NoError(t, tf.Flush())

	// Read back from first offset
	r1 := tf.NewReader(off1, int64(len(data1)), 4096)
	buf1 := make([]byte, len(data1))
	_, err = io.ReadFull(r1, buf1)
	require.NoError(t, err)
	assert.Equal(t, data1, buf1)

	// Read back from second offset
	r2 := tf.NewReader(off2, int64(len(data2)), 4096)
	buf2 := make([]byte, len(data2))
	_, err = io.ReadFull(r2, buf2)
	require.NoError(t, err)
	assert.Equal(t, data2, buf2)
}

func TestTempFileReset(t *testing.T) {
	tf, err := NewTempFile("", 4096)
	require.NoError(t, err)
	defer tf.Close()

	_, err = tf.Write([]byte("some data"))
	require.NoError(t, err)
	require.NoError(t, tf.Flush())

	require.NoError(t, tf.Reset())
	assert.Equal(t, int64(0), tf.offset)

	// Should be able to write again from the start
	off, err := tf.Write([]byte("new data"))
	require.NoError(t, err)
	assert.Equal(t, int64(0), off)
}
