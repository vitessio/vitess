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
package stats

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestStatsdHook(t *testing.T) {
	t.Run("RegisterTimerHook", func(t *testing.T) {
		defaultStatsdHook = statsdHook{}

		// Create a dummy timerHook function
		dummyTimerHook := func(name, tags string, value int64, timings *Timings) {
			assert.Equal(t, "dummyName", name)
			assert.Equal(t, "dummyTags", tags)
			assert.Equal(t, int64(42), value)
		}

		// Register the dummy timerHook and then call the same
		RegisterTimerHook(dummyTimerHook)

		assert.NotNil(t, defaultStatsdHook.timerHook)
		assert.Nil(t, defaultStatsdHook.histogramHook)
		defaultStatsdHook.timerHook("dummyName", "dummyTags", 42, nil)
	})

	t.Run("RegisterHistogramHook", func(t *testing.T) {
		defaultStatsdHook = statsdHook{}

		// Create a dummy histogramHook function
		dummyHistogramHook := func(name string, value int64) {
			assert.Equal(t, "dummyName", name)
			assert.Equal(t, int64(42), value)
		}

		RegisterHistogramHook(dummyHistogramHook)

		assert.NotNil(t, defaultStatsdHook.histogramHook)
		assert.Nil(t, defaultStatsdHook.timerHook)
		defaultStatsdHook.histogramHook("dummyName", 42)
	})
}
