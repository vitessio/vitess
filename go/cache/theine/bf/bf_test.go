/*
Copyright 2023 The Vitess Authors.
Copyright 2023 Yiling-J

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

package bf

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBloom(t *testing.T) {
	bf := New(0.1)
	bf.EnsureCapacity(500)

	exist := bf.Insert(123)
	require.False(t, exist)

	exist = bf.Exist(123)
	require.True(t, exist)

	exist = bf.Exist(456)
	require.False(t, exist)

	bf = New(0.01)
	require.Equal(t, 512, bf.Capacity)
	require.Equal(t, 0.01, bf.FalsePositiveRate)

	bf.Insert(123)
	exist = bf.Exist(123)
	require.True(t, exist)

	bf.Insert(256)
	exist = bf.Exist(256)
	require.True(t, exist)

	bf.Reset()

	exist = bf.Exist(123)
	require.False(t, exist)

	exist = bf.Exist(256)
	require.False(t, exist)
}
