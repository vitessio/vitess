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

package evalengine

import (
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestParseHexNumberLeavesInputIntact pins that decoding an odd-length 0x
// literal does not write to its input. The bytes are the shared bvar.Value:
// Concatenate.parallelExec hands the same map to every route, so a write to
// pad the literal in place is visible to the sibling routes, which then see a
// literal without its 0x prefix. hex.DecodeBytes pads odd input itself.
func TestParseHexNumberLeavesInputIntact(t *testing.T) {
	want, err := parseHexNumber([]byte("0x01FF"))
	require.NoError(t, err)

	val := []byte("0x1FF")
	got, err := parseHexNumber(val)
	require.NoError(t, err)
	assert.Equal(t, want, got, "an odd digit count decodes as if left-padded with 0")
	assert.Equal(t, []byte("0x1FF"), val)

	// A concurrent reader must never observe the literal changing under it.
	var mutated atomic.Bool
	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Go(func() {
		for {
			select {
			case <-stop:
				return
			default:
				if val[1] != 'x' {
					mutated.Store(true)
				}
			}
		}
	})
	for range 100000 {
		_, err := parseHexNumber(val)
		require.NoError(t, err)
	}
	close(stop)
	wg.Wait()
	assert.False(t, mutated.Load(), "parseHexNumber wrote to its input")
}

// TestMalformedLiteralErrorsCapEcho pins that the malformed-literal errors do
// not echo an unbounded caller-controlled payload: the payload is cut at 64
// bytes and marked, matching sqltypes.rawLiteral.
func TestMalformedLiteralErrorsCapEcho(t *testing.T) {
	long := []byte("1" + strings.Repeat("A", 200))
	for name, parse := range map[string]func([]byte) ([]byte, error){
		"hexnum": parseHexNumber,
		"hexval": parseHexValLiteral,
		"bitnum": parseBitNum,
	} {
		t.Run(name, func(t *testing.T) {
			_, err := parse(long)
			require.Error(t, err)
			require.ErrorContains(t, err, "malformed")
			require.ErrorContains(t, err, "…")
			assert.NotContains(t, err.Error(), string(long))
			assert.Less(t, len(err.Error()), 160)
		})
	}
	// parseBitNum's second message (bad digits after a good prefix) takes the
	// same path.
	_, err := parseBitNum([]byte("0b" + strings.Repeat("2", 200)))
	require.Error(t, err)
	require.ErrorContains(t, err, "not base 2")
	require.ErrorContains(t, err, "…")
	assert.Less(t, len(err.Error()), 160)
}
