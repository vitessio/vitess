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
	"sync"
	"testing"
	"time"

	"github.com/fsnotify/fsnotify"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/viperutil"
	vipersync "vitess.io/vitess/go/viperutil/internal/sync"
	"vitess.io/vitess/go/viperutil/internal/value"
)

func TestWatchConfig(t *testing.T) {
	type config struct {
		A, B int
	}

	writeConfig := func(tmp *os.File, a, b int) error {
		stat, err := os.Stat(tmp.Name())
		if err != nil {
			return err
		}

		data, err := json.Marshal(&config{A: a, B: b})
		if err != nil {
			return err
		}

		err = os.WriteFile(tmp.Name(), data, stat.Mode())
		if err != nil {
			return err
		}

		data, err = os.ReadFile(tmp.Name())
		if err != nil {
			return err
		}

		var cfg config
		if err := json.Unmarshal(data, &cfg); err != nil {
			return err
		}

		if cfg.A != a || cfg.B != b {
			return fmt.Errorf("config did not persist; want %+v got %+v", config{A: a, B: b}, cfg)
		}

		return nil
	}
	writeRandomConfig := func(tmp *os.File) error {
		a, b := rand.Intn(100), rand.Intn(100)
		return writeConfig(tmp, a, b)
	}

	tmp, err := os.CreateTemp(t.TempDir(), "TestWatchConfig_*.json")
	require.NoError(t, err)

	require.NoError(t, writeRandomConfig(tmp))

	v := viper.New()
	v.SetConfigFile(tmp.Name())
	require.NoError(t, v.ReadInConfig())

	wCh, rCh := make(chan struct{}), make(chan struct{})
	v.OnConfigChange(func(event fsnotify.Event) {
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
	require.NoError(t, writeConfig(tmp, a+1, b+1))
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
		require.NoError(t, writeRandomConfig(tmp))
		time.Sleep(writeJitter())
	}

	cancel()
	wg.Wait()
}

func jitter(min, max int) int {
	return min + rand.Intn(max-min+1)
}
