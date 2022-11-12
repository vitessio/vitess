/*
Copyright 2022 The Vitess Authors.

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
	"errors"
	"fmt"
	"sync"

	"github.com/fsnotify/fsnotify"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
)

// Viper is a wrapper around a pair of viper.Viper instances to provide config-
// reloading in a threadsafe manner.
//
// It maintains one viper, called "disk", which does the actual config watch and
// reload (via viper's WatchConfig), and a second viper, called "live", which
// Values (registered via viperutil.Configure with Dynamic=true) access their
// settings from. The "live" config only updates after blocking all values from
// reading in order to swap in the most recently-loaded config from the "disk".
type Viper struct {
	m    sync.Mutex // prevents races between loadFromDisk and AllSettings
	disk *viper.Viper
	live *viper.Viper
	keys map[string]*sync.RWMutex

	subscribers    []chan<- struct{}
	watchingConfig bool
}

// New returns a new synced Viper.
func New() *Viper {
	return &Viper{
		disk: viper.New(),
		live: viper.New(),
		keys: map[string]*sync.RWMutex{},
	}
}

// ErrDuplicateWatch is returned when Watch is called on a synced Viper which
// has already started a watch.
var ErrDuplicateWatch = errors.New("duplicate watch")

// Watch starts watching the config used by the passed-in Viper. Before starting
// the watch, the synced viper will perform an initial read and load from disk
// so that the live config is ready for use without requiring an initial config
// change.
//
// If the given static viper did not load a config file (and is instead relying
// purely on defaults, flags, and environment variables), then the settings of
// that viper are merged over, and this synced Viper may be used to set up an
// actual watch later.
//
// If this synced viper is already watching a config file, this function returns
// an ErrDuplicateWatch. Other errors may be returned via underlying viper code
// to ensure the config file can be read in properly.
func (v *Viper) Watch(static *viper.Viper) error {
	if v.watchingConfig {
		return fmt.Errorf("%w: viper is already watching %s", ErrDuplicateWatch, v.disk.ConfigFileUsed())
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

// Notify adds a subscription that this synced viper will attempt to notify on
// config changes, after the updated config has been copied over from disk to
// live.
//
// Analogous to signal.Notify, notifications are sent non-blocking, so users
// should account for this when consuming from the channel they've provided.
//
// This function must be called prior to setting up a Watch; it will panic if a
// a watch has already been established on this synced Viper.
func (v *Viper) Notify(ch chan<- struct{}) {
	if v.watchingConfig {
		panic("cannot Notify after starting to watch a config")
	}

	v.subscribers = append(v.subscribers, ch)
}

// AllSettings returns the current live settings.
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

// AdaptGetter wraps a get function (matching the signature of
// viperutil.Options.GetFunc) to be threadsafe with the passed-in synced Viper.
//
// It must be called prior to starting a watch on the synced Viper; it will
// panic if a watch has already been established.
//
// This function must be called at most once per key; it will panic if attempting
// to adapt multiple getters for the same key.
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
