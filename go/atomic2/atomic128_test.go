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

package atomic2

import (
	"testing"
	"unsafe"

	"github.com/stretchr/testify/require"
)

func TestCompareAndSwap(t *testing.T) {
	i1 := new(int)
	i2 := new(int)
	n := &PointerAndUint64[int]{p: unsafe.Pointer(i1), u: 12345}

	ok := n.CompareAndSwap(i1, 12345, i2, 67890)
	require.Truef(t, ok, "unexpected CAS failure")

	pp, uu := n.Load()
	require.Equal(t, i2, pp)
	require.Equal(t, uint64(67890), uu)

	ok = n.CompareAndSwap(i1, 12345, nil, 0)
	require.Falsef(t, ok, "unexpected CAS success")

	pp, uu = n.Load()
	require.Equal(t, pp, i2)
	require.Equal(t, uu, uint64(67890))
}
