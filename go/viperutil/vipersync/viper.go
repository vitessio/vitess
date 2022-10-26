package vipersync

import (
	"errors"
	"fmt"
	"sync"

	"github.com/fsnotify/fsnotify"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"

	"vitess.io/vitess/go/viperutil"
)

// TODO: document all this

type Viper struct {
	disk *viper.Viper
	live *viper.Viper
	keys map[string]lockable

	subscribers    []chan<- struct{}
	watchingConfig bool
}

// lockable represents a structure that contains a lockable field.
//
// we need this interface in order to store our map of synced keys in the synced
// viper. unfortunately, we cannot do
//
//	for _, key := range sv.keys {
//		key.m.Lock()
//		defer key.m.Unlock()
//	}
//
// because generics don't (yet?) allow a collection of _different_ generic types
// (e.g. you can't have []T{1, "hello", false}, because they are not all the same T),
// so we abstract over the interface. it's not exported because no one outside
// this package should be accessing a synced key's mutex.
type lockable interface {
	locker() sync.Locker
}

type syncedKey[T any] struct {
	m   sync.RWMutex
	get func(key string) T
}

func (sk *syncedKey[T]) locker() sync.Locker {
	return &sk.m
}

func NewViper(v *viper.Viper) *Viper {
	sv := &Viper{
		disk: v,
		live: viper.New(),
		keys: map[string]lockable{},
	}

	sv.disk.OnConfigChange(func(in fsnotify.Event) {
		// Inform each key that an update is coming.
		for _, key := range sv.keys {
			key.locker().Lock()
			// This won't fire until after the config has been updated on sv.live.
			defer key.locker().Unlock()
		}

		// Now, every key is blocked from reading; we can atomically swap the
		// config on disk for the config in memory.
		sv.loadFromDisk()

		for _, ch := range sv.subscribers {
			select {
			case ch <- struct{}{}:
			default:
			}
		}
	})

	return sv
}

func (v *Viper) Watch(cfg string) error {
	if v.watchingConfig {
		// TODO: declare an exported error and include the filename in wrapped
		// error.
		return errors.New("duplicate watch")
	}

	if err := viperutil.ReadInConfig(v.disk, viperutil.ConfigOptions{
		File: cfg,
	}, viperutil.ErrorOnConfigFileNotFound); err != nil {
		return err // TODO: wrap error
	}

	v.watchingConfig = true
	v.loadFromDisk()
	v.disk.WatchConfig()

	return nil
}

func (v *Viper) loadFromDisk() {
	// Fun fact! MergeConfigMap actually only ever returns nil. Maybe in an
	// older version of viper it used to actually handle errors, but now it
	// decidedly does not. See https://github.com/spf13/viper/blob/v1.8.1/viper.go#L1492-L1499.
	_ = v.live.MergeConfigMap(v.disk.AllSettings())
}

func (v *Viper) Notify(ch chan<- struct{}) {
	if v.watchingConfig {
		panic("cannot Notify after starting to watch config")
	}

	v.subscribers = append(v.subscribers, ch)
}

func BindValue[T any](v *Viper, value *viperutil.Value[T], fs *pflag.FlagSet) {
	value.Bind(v.live, fs)
}

func AdaptGetter[T any](v *Viper, key string, getter func(v *viper.Viper) func(key string) T) func(key string) T {
	if v.watchingConfig {
		panic("cannot adapt getter to synchronized viper which is already watching a config")
	}

	if _, ok := v.keys[key]; ok {
		panic(fmt.Sprintf("already adapted a getter for key %s", key))
	}

	sk := &syncedKey[T]{
		get: getter(v.live),
	}

	v.keys[key] = sk

	return func(key string) T {
		sk.m.RLock()
		defer sk.m.RUnlock()

		return sk.get(key)
	}
}
