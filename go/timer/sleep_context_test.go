/*
Copyright 2020 The Vitess Authors.

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

package timer

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSleepContext(t *testing.T) {
	ctx := context.Background()
	start := time.Now()
	err := SleepContext(ctx, 10*time.Millisecond)
	require.NoError(t, err)
	assert.True(t, time.Since(start) > 10*time.Millisecond, time.Since(start))
	assert.True(t, time.Since(start) < 100*time.Millisecond, time.Since(start))

	ctx, cancel := context.WithTimeout(ctx, 10*time.Millisecond)
	defer cancel()
	start = time.Now()
	err = SleepContext(ctx, 100*time.Millisecond)
	require.Error(t, err)
	assert.True(t, time.Since(start) > 10*time.Millisecond, time.Since(start))
	assert.True(t, time.Since(start) < 100*time.Millisecond, time.Since(start))
}
