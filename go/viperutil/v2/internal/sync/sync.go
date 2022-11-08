package sync

import (
	"errors"
	"fmt"
	"sync"

	"github.com/fsnotify/fsnotify"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
)

type Viper struct {
	m    sync.Mutex // prevents races between loadFromDisk and AllSettings
	disk *viper.Viper
	live *viper.Viper
	keys map[string]*sync.RWMutex

	subscribers    []chan<- struct{}
	watchingConfig bool
}

func New() *Viper {
	return &Viper{
		disk: viper.New(),
		live: viper.New(),
		keys: map[string]*sync.RWMutex{},
	}
}

func (v *Viper) Watch(static *viper.Viper) error {
	if v.watchingConfig {
		// TODO: declare an exported error and include the filename in wrapped
		// error.
		return errors.New("duplicate watch")
	}

	cfg := static.ConfigFileUsed()
	if cfg == "" {
		// No config file to watch, just merge the settings and return.
		return v.live.MergeConfigMap(static.AllSettings())
	}

	v.disk.SetConfigFile(cfg)
	if err := v.disk.ReadInConfig(); err != nil {
		return err
	}

	v.watchingConfig = true
	v.loadFromDisk()
	v.disk.OnConfigChange(func(in fsnotify.Event) {
		for _, m := range v.keys {
			m.Lock()
			// This won't fire until after the config has been updated on v.live.
			defer m.Unlock()
		}

		v.loadFromDisk()

		for _, ch := range v.subscribers {
			select {
			case ch <- struct{}{}:
			default:
			}
		}
	})
	v.disk.WatchConfig()

	return nil
}

func (v *Viper) Notify(ch chan<- struct{}) {
	if v.watchingConfig {
		panic("cannot Notify after starting to watch a config")
	}

	v.subscribers = append(v.subscribers, ch)
}

func (v *Viper) AllSettings() map[string]any {
	v.m.Lock()
	defer v.m.Unlock()

	return v.live.AllSettings()
}

func (v *Viper) loadFromDisk() {
	v.m.Lock()
	defer v.m.Unlock()

	// Fun fact! MergeConfigMap actually only ever returns nil. Maybe in an
	// older version of viper it used to actually handle errors, but now it
	// decidedly does not. See https://github.com/spf13/viper/blob/v1.8.1/viper.go#L1492-L1499.
	_ = v.live.MergeConfigMap(v.disk.AllSettings())
}

// begin implementation of registry.Bindable for sync.Viper

func (v *Viper) BindEnv(vars ...string) error                 { return v.live.BindEnv(vars...) }
func (v *Viper) BindPFlag(key string, flag *pflag.Flag) error { return v.live.BindPFlag(key, flag) }
func (v *Viper) RegisterAlias(alias string, key string)       { v.live.RegisterAlias(alias, key) }
func (v *Viper) SetDefault(key string, value any)             { v.live.SetDefault(key, value) }

// end implementation of registry.Bindable for sync.Viper

func AdaptGetter[T any](key string, getter func(v *viper.Viper) func(key string) T, v *Viper) func(key string) T {
	if v.watchingConfig {
		panic("cannot adapt getter to synchronized viper which is already watching a config")
	}

	if _, ok := v.keys[key]; ok {
		panic(fmt.Sprintf("already adapted a getter for key %s", key))
	}

	var m sync.RWMutex
	v.keys[key] = &m

	boundGet := getter(v.live)

	return func(key string) T {
		m.RLock()
		defer m.RUnlock()

		return boundGet(key)
	}
}
