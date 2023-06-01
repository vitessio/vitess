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

package sync_test

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/fsnotify/fsnotify"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/viperutil"
	vipersync "vitess.io/vitess/go/viperutil/internal/sync"
	"vitess.io/vitess/go/viperutil/internal/value"
)

func TestPersistConfig(t *testing.T) {
	t.Skip("temporarily skipping this to unblock PRs since it's flaky")
	type config struct {
		Foo int `json:"foo"`
	}

	loadConfig := func(t *testing.T, f *os.File) config {
		t.Helper()

		data, err := os.ReadFile(f.Name())
		require.NoError(t, err)

		var cfg config
		require.NoError(t, json.Unmarshal(data, &cfg))

		return cfg
	}

	setup := func(t *testing.T, v *vipersync.Viper, minWaitInterval time.Duration) (*os.File, chan struct{}) {
		tmp, err := os.CreateTemp(t.TempDir(), fmt.Sprintf("%s_*.json", strings.ReplaceAll(t.Name(), "/", "_")))
		require.NoError(t, err)

		t.Cleanup(func() { os.Remove(tmp.Name()) })

		cfg := config{
			Foo: jitter(1, 100),
		}

		data, err := json.Marshal(&cfg)
		require.NoError(t, err)

		_, err = tmp.Write(data)
		require.NoError(t, err)

		static := viper.New()
		static.SetConfigFile(tmp.Name())
		require.NoError(t, static.ReadInConfig())

		ch := make(chan struct{}, 1)
		v.Notify(ch)

		cancel, err := v.Watch(context.Background(), static, minWaitInterval)
		require.NoError(t, err)
		t.Cleanup(cancel)

		return tmp, ch
	}

	t.Run("basic", func(t *testing.T) {
		v := vipersync.New()

		minPersistWaitInterval := 10 * time.Second
		get := vipersync.AdaptGetter("foo", viperutil.GetFuncForType[int](), v)
		f, ch := setup(t, v, minPersistWaitInterval)

		old := get("foo")
		loadConfig(t, f)
		v.Set("foo", old+1)
		// This should happen immediately in-memory and on-disk.
		assert.Equal(t, old+1, get("foo"))
		<-ch
		assert.Equal(t, old+1, loadConfig(t, f).Foo)

		v.Set("foo", old+2)
		// This should _also_ happen immediately in-memory, but not on-disk.
		// It will take up to 2 * minPersistWaitInterval to reach the disk.
		assert.Equal(t, old+2, get("foo"))
		assert.Equal(t, old+1, loadConfig(t, f).Foo)

		select {
		case <-ch:
		case <-time.After(2 * minPersistWaitInterval):
			assert.Fail(t, "config was not persisted quickly enough", "config took longer than %s to persist (minPersistWaitInterval = %s)", 2*minPersistWaitInterval, minPersistWaitInterval)
		}

		assert.Equal(t, old+2, loadConfig(t, f).Foo)
	})

	t.Run("no persist interval", func(t *testing.T) {
		v := vipersync.New()

		var minPersistWaitInterval time.Duration
		get := vipersync.AdaptGetter("foo", viperutil.GetFuncForType[int](), v)
		f, ch := setup(t, v, minPersistWaitInterval)

		old := get("foo")
		loadConfig(t, f)
		v.Set("foo", old+1)
		// This should happen immediately in-memory and on-disk.
		assert.Equal(t, old+1, get("foo"))
		<-ch
		assert.Equal(t, old+1, loadConfig(t, f).Foo)

		v.Set("foo", old+2)
		// This should _also_ happen immediately in-memory, and on-disk.
		assert.Equal(t, old+2, get("foo"))
		<-ch
		assert.Equal(t, old+2, loadConfig(t, f).Foo)
	})
}

func TestWatchConfig(t *testing.T) {
	type config struct {
		A, B int
	}

	tmp, err := os.CreateTemp(".", "TestWatchConfig_*.json")
	require.NoError(t, err)
	t.Cleanup(func() { os.Remove(tmp.Name()) })

	stat, err := os.Stat(tmp.Name())
	require.NoError(t, err)

	writeConfig := func(a, b int) error {
		data, err := json.Marshal(&config{A: a, B: b})
		if err != nil {
			return err
		}

		return os.WriteFile(tmp.Name(), data, stat.Mode())
	}
	writeRandomConfig := func() error {
		a, b := rand.Intn(100), rand.Intn(100)
		return writeConfig(a, b)
	}

	require.NoError(t, writeRandomConfig())

	v := viper.New()
	v.SetConfigFile(tmp.Name())
	require.NoError(t, v.ReadInConfig())

	wCh, rCh := make(chan struct{}), make(chan struct{})
	v.OnConfigChange(func(in fsnotify.Event) {
		select {
		case <-rCh:
			return
		default:
		}

		wCh <- struct{}{}
		// block forever to prevent this viper instance from double-updating.
		<-rCh
	})
	v.WatchConfig()

	// Make sure that basic, unsynchronized WatchConfig is set up before
	// beginning the actual test.
	a, b := v.GetInt("a"), v.GetInt("b")
	require.NoError(t, writeConfig(a+1, b+1))
	<-wCh // wait for the update to finish

	require.Equal(t, a+1, v.GetInt("a"))
	require.Equal(t, b+1, v.GetInt("b"))

	rCh <- struct{}{}

	sv := vipersync.New()
	A := viperutil.Configure("a", viperutil.Options[int]{Dynamic: true})
	B := viperutil.Configure("b", viperutil.Options[int]{Dynamic: true})

	A.(*value.Dynamic[int]).Base.BoundGetFunc = vipersync.AdaptGetter("a", func(v *viper.Viper) func(key string) int {
		return v.GetInt
	}, sv)
	B.(*value.Dynamic[int]).Base.BoundGetFunc = vipersync.AdaptGetter("b", func(v *viper.Viper) func(key string) int {
		return v.GetInt
	}, sv)

	cancel, err := sv.Watch(context.Background(), v, 0)
	require.NoError(t, err)
	defer cancel()

	var wg sync.WaitGroup
	ctx, cancel := context.WithCancel(context.Background())

	// Sleep between 25 and 50ms between reads.
	readJitter := func() time.Duration {
		return time.Duration(jitter(25, 50)) * time.Millisecond
	}
	// Sleep between 75 and 125ms between writes.
	writeJitter := func() time.Duration {
		return time.Duration(jitter(75, 125)) * time.Millisecond
	}

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()

			for {
				select {
				case <-ctx.Done():
					return
				default:
				}

				switch i % 2 {
				case 0:
					A.Get()
				case 1:
					B.Get()
				}

				time.Sleep(readJitter())
			}
		}(i)
	}

	for i := 0; i < 100; i++ {
		require.NoError(t, writeRandomConfig())
		time.Sleep(writeJitter())
	}

	cancel()
	wg.Wait()
}

func jitter(min, max int) int {
	return min + rand.Intn(max-min+1)
}
