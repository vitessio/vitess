package vipersync

import (
	"fmt"

	"github.com/fsnotify/fsnotify"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"

	"vitess.io/vitess/go/viperutil"
)

// TODO: document all this

type Viper struct {
	disk *viper.Viper
	live *viper.Viper
	keys map[string]*syncedKey

	watchingConfig bool
}

type syncedKey struct {
	updates chan chan struct{}
}

func NewViper(v *viper.Viper) *Viper {
	sv := &Viper{
		disk: v,
		live: viper.New(),
		keys: map[string]*syncedKey{},
	}

	// Fun fact! MergeConfigMap actually only ever returns nil. Maybe in an
	// older version of viper it used to actually handle errors, but now it
	// decidedly does not. See https://github.com/spf13/viper/blob/v1.8.1/viper.go#L1492-L1499.
	_ = sv.live.MergeConfigMap(sv.disk.AllSettings())
	sv.disk.OnConfigChange(func(in fsnotify.Event) {
		chs := make([]chan struct{}, 0, len(sv.keys))
		// Inform each key that an update is coming.
		for _, key := range sv.keys {
			select {
			case <-key.updates:
				// The previous update channel never got used by a reader.
				// This means that no calls to a Get* func for that key happened
				// between the last config change and now.
			default:
			}

			// Create a new channel for signalling the current config change.
			ch := make(chan struct{})
			key.updates <- ch
			chs = append(chs, ch)
		}

		// Now, every key is blocked for reading; we can atomically swap the
		// config on disk for the config in memory.
		_ = sv.live.MergeConfigMap(sv.disk.AllSettings())

		// Unblock each key until the next update.
		for _, ch := range chs {
			close(ch)
		}
	})

	return sv
}

func (v *Viper) WatchConfig() {
	if v.watchingConfig {
		panic("WatchConfig called twice on synchronized viper")
	}

	v.watchingConfig = true
	v.disk.WatchConfig()
}

func BindValue[T any](v *Viper, value *viperutil.Value[T], fs *pflag.FlagSet) {
	value.Bind(v.live, fs)
}

func AdaptGetter[T any](v *Viper, key string, getter func(v *viper.Viper) T) func(key string) T {
	if v.watchingConfig {
		panic("cannot adapt getter to synchronized viper which is already watching a config")
	}

	if _, ok := v.keys[key]; ok {
		panic(fmt.Sprintf("already adapted a getter for key %s", key))
	}

	sk := &syncedKey{
		updates: make(chan chan struct{}, 1),
	}

	v.keys[key] = sk

	return func(key string) T {
		select {
		case update := <-sk.updates:
			// There's an update in progress, wait for channel close before
			// reading.
			<-update
			return getter(v.live)
		default:
			// No ongoing update, read.
			return getter(v.live)
		}
	}
}
