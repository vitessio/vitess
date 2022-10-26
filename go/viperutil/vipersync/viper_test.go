package vipersync_test

import (
	"context"
	"encoding/json"
	"math/rand"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/fsnotify/fsnotify"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/viperutil"
	"vitess.io/vitess/go/viperutil/vipersync"
)

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

	time.Sleep(time.Millisecond * 1000)
	require.Equal(t, a+1, v.GetInt("a"))
	require.Equal(t, b+1, v.GetInt("b"))

	rCh <- struct{}{}

	// Now, set up our synchronized viper and do a bunch of concurrent reads/writes.
	v = viper.New()

	sv := vipersync.New()
	A := viperutil.NewValue("a",
		vipersync.AdaptGetter(sv, "a", func(v *viper.Viper) func(key string) int {
			return v.GetInt
		}),
	)
	B := viperutil.NewValue("b",
		vipersync.AdaptGetter(sv, "b", func(v *viper.Viper) func(key string) int {
			return v.GetInt
		}),
	)
	vipersync.BindValue(sv, A, nil)
	vipersync.BindValue(sv, B, nil)

	require.NoError(t, sv.Watch(tmp.Name()))

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

	for i := 0; i < 2; i++ {
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
					A.Fetch()
				case 1:
					B.Fetch()
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
