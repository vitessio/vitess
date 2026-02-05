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

package debug

import (
	"math/rand/v2"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSanitizeString(t *testing.T) {
	t.Parallel()

	t.Run("empty string", func(t *testing.T) {
		t.Parallel()

		out := SanitizeString("")
		assert.Equal(t, "", out)
	})

	t.Run("non-empty strings", func(t *testing.T) {
		t.Parallel()

		letters := "abcdefghijklmnopqrstuvwxyz0123456789"

		for i := 0; i < 10; i++ {
			t.Run("", func(t *testing.T) {
				t.Parallel()

				length := rand.IntN(20) + 1 // [1, 21)
				word := ""
				for j := 0; j < length; j++ {
					k := rand.IntN(len(letters))
					word += letters[k : k+1]
				}

				out := SanitizeString(word)
				assert.Equal(t, sanitized, out)
			})
		}
	})
}

func TestTimeToString(t *testing.T) {
	t.Parallel()

	for i := 0; i < 10; i++ {
		t.Run("", func(t *testing.T) {
			t.Parallel()

			start := time.Now()
			secondsoff := rand.IntN(60)
			minutesoff := rand.IntN(60)

			in := start.Add(time.Second*time.Duration(secondsoff) + time.Minute*time.Duration(minutesoff))
			out, err := time.Parse(time.RFC3339, TimeToString(in))
			require.NoError(t, err, "failed to round-trip a time through debug.TimeToString")
			in = in.Truncate(time.Second) // RFC3339 does not include millis
			assert.True(t, in.Equal(out), "round-trip changed the time value; in: %v, out: %v", in, out)
		})
	}
}
