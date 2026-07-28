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

package sync

import (
	"encoding/json"
	"math/rand/v2"
	"testing"
	"time"

	"github.com/spf13/afero"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPersistConfig(t *testing.T) {
	type config struct {
		Foo int `json:"foo"`
	}

	loadConfig := func(t *testing.T, fs afero.Fs) config {
		t.Helper()

		data, err := afero.ReadFile(fs, "config.json")
		require.NoError(t, err)

		var cfg config
		require.NoError(t, json.Unmarshal(data, &cfg))

		return cfg
	}

	setup := func(t *testing.T, v *Viper, minWaitInterval time.Duration) (afero.Fs, <-chan time.Time) {
		t.Helper()

		fs := afero.NewMemMapFs()
		cfg := config{
			Foo: jitter(1, 100),
		}

		data, err := json.Marshal(&cfg)
		require.NoError(t, err)

		err = afero.WriteFile(fs, "config.json", data, 0o644)
		require.NoError(t, err)

		static := viper.New()
		static.SetFs(fs)
		static.SetConfigFile("config.json")

		require.NoError(t, static.ReadInConfig())
		require.Equal(t, cfg.Foo, static.GetInt("foo"))

		// The hook runs synchronously in the persist goroutine right after
		// each disk write, so the timestamps it sends can be used to verify
		// the spacing between writes.
		ch := make(chan time.Time, 1)
		v.onConfigWrite = func() { ch <- time.Now() }
		v.SetFs(fs)

		cancel, err := v.Watch(t.Context(), static, minWaitInterval)
		require.NoError(t, err)

		t.Cleanup(cancel)
		return fs, ch
	}

	t.Run("basic", func(t *testing.T) {
		v := New()

		minPersistWaitInterval := 500 * time.Millisecond
		get := AdaptGetter("foo", func(v *viper.Viper) func(key string) int { return v.GetInt }, v)
		fs, ch := setup(t, v, minPersistWaitInterval)

		old := get("foo")
		loadConfig(t, fs)
		v.Set("foo", old+1)
		// This should happen immediately in-memory, and on-disk once the
		// initial wait interval elapses.
		assert.Equal(t, old+1, get("foo"))
		firstPersist := <-ch
		assert.Equal(t, old+1, loadConfig(t, fs).Foo)

		v.Set("foo", old+2)
		// This should _also_ happen immediately in-memory, but the on-disk
		// write is debounced by minPersistWaitInterval.
		assert.Equal(t, old+2, get("foo"))

		// Every disk write signals ch, so the next signal must be the second
		// persist. Its timestamp being at least minPersistWaitInterval after
		// the first proves the debounce: this holds regardless of scheduling
		// pauses, which can only widen the gap, never shrink it.
		select {
		case secondPersist := <-ch:
			assert.GreaterOrEqual(t, secondPersist.Sub(firstPersist), minPersistWaitInterval)
		case <-time.After(30 * time.Second):
			assert.Fail(t, "config was not persisted quickly enough", "config took longer than 30s to persist (minPersistWaitInterval = %s)", minPersistWaitInterval)
		}

		assert.Equal(t, old+2, loadConfig(t, fs).Foo)
	})

	t.Run("no persist interval", func(t *testing.T) {
		v := New()

		var minPersistWaitInterval time.Duration
		get := AdaptGetter("foo", func(v *viper.Viper) func(key string) int { return v.GetInt }, v)
		fs, ch := setup(t, v, minPersistWaitInterval)

		old := get("foo")
		loadConfig(t, fs)
		v.Set("foo", old+1)
		// This should happen immediately in-memory and on-disk.
		assert.Equal(t, old+1, get("foo"))
		<-ch
		assert.Equal(t, old+1, loadConfig(t, fs).Foo)

		v.Set("foo", old+2)
		// This should _also_ happen immediately in-memory, and on-disk.
		assert.Equal(t, old+2, get("foo"))
		<-ch
		assert.Equal(t, old+2, loadConfig(t, fs).Foo)
	})
}

func jitter(min, max int) int {
	return min + rand.IntN(max-min+1)
}
