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

package value

import (
	"fmt"

	"github.com/spf13/pflag"
	"github.com/spf13/viper"

	"vitess.io/vitess/go/viperutil/internal/registry"
	"vitess.io/vitess/go/viperutil/internal/sync"
	"vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
)

// Registerable is the subset of the interface exposed by Values (which is
// declared in the public viperutil package).
//
// We need a separate interface type because Go generics do not let you define
// a function that takes Value[T] for many, different T's, which we want to do
// for BindFlags.
type Registerable interface {
	Key() string
	Registry() registry.Bindable
	Flag(fs *pflag.FlagSet) (*pflag.Flag, error)
}

// Base is the base functionality shared by Static and Dynamic values. It
// implements viperutil.Value.
type Base[T any] struct {
	KeyName    string
	DefaultVal T

	GetFunc      func(v *viper.Viper) func(key string) T
	BoundGetFunc func(key string) T

	Aliases  []string
	FlagName string
	EnvVars  []string
}

func (val *Base[T]) Key() string { return val.KeyName }
func (val *Base[T]) Default() T  { return val.DefaultVal }
func (val *Base[T]) Get() T      { return val.BoundGetFunc(val.Key()) }

// ErrNoFlagDefined is returned when a Value has a FlagName set, but the given
// FlagSet does not define a flag with that name.
var ErrNoFlagDefined = vterrors.New(vtrpc.Code_INVALID_ARGUMENT, "flag not defined")

// Flag is part of the Registerable interface. If the given flag set has a flag
// with the name of this value's configured flag, that flag is returned, along
// with a nil error. If no flag exists on the flag set with that name, an error
// is returned.
//
// If the value is not configured to correspond to a flag (FlagName == ""), then
// (nil, nil) is returned.
func (val *Base[T]) Flag(fs *pflag.FlagSet) (*pflag.Flag, error) {
	if val.FlagName == "" {
		return nil, nil
	}

	flag := fs.Lookup(val.FlagName)
	if flag == nil {
		return nil, vterrors.Wrapf(ErrNoFlagDefined, "%s with name %s (for key %s)", ErrNoFlagDefined.Error(), val.FlagName, val.Key())
	}

	return flag, nil
}

func (val *Base[T]) bind(v registry.Bindable) {
	v.SetDefault(val.Key(), val.DefaultVal)

	for _, alias := range val.Aliases {
		v.RegisterAlias(alias, val.Key())
	}

	if len(val.EnvVars) > 0 {
		vars := append([]string{val.Key()}, val.EnvVars...)
		_ = v.BindEnv(vars...)
	}
}

// BindFlags creates bindings between each value's registry and the given flag
// set. This function will panic if any of the values defines a flag that does
// not exist in the flag set.
func BindFlags(fs *pflag.FlagSet, values ...Registerable) {
	for _, val := range values {
		flag, err := val.Flag(fs)
		switch {
		case err != nil:
			panic(fmt.Errorf("failed to load flag for %s: %w", val.Key(), err))
		case flag == nil:
			continue
		}

		_ = val.Registry().BindPFlag(val.Key(), flag)
		if flag.Name != val.Key() {
			val.Registry().RegisterAlias(flag.Name, val.Key())
		}
	}
}

// Static is a static value. Static values register to the Static registry, and
// do not respond to changes to config files. Their Get() method will return the
// same value for the lifetime of the process.
type Static[T any] struct {
	*Base[T]
}

// NewStatic returns a static value derived from the given base value, after
// binding it to the static registry.
func NewStatic[T any](base *Base[T]) *Static[T] {
	base.bind(registry.Static)
	base.BoundGetFunc = base.GetFunc(registry.Static)

	return &Static[T]{
		Base: base,
	}
}

func (val *Static[T]) Registry() registry.Bindable {
	return registry.Static
}

func (val *Static[T]) Set(v T) {
	registry.Static.Set(val.KeyName, v)
}

// Dynamic is a dynamic value. Dynamic values register to the Dynamic registry,
// and respond to changes to watched config files. Their Get() methods will
// return whatever value is currently live in the config, in a threadsafe
// manner.
type Dynamic[T any] struct {
	*Base[T]
}

// NewDynamic returns a dynamic value derived from the given base value, after
// binding it to the dynamic registry and wrapping its GetFunc to be threadsafe
// with respect to config reloading.
func NewDynamic[T any](base *Base[T]) *Dynamic[T] {
	base.bind(registry.Dynamic)
	base.BoundGetFunc = sync.AdaptGetter(base.Key(), base.GetFunc, registry.Dynamic)

	return &Dynamic[T]{
		Base: base,
	}
}

func (val *Dynamic[T]) Registry() registry.Bindable {
	return registry.Dynamic
}

func (val *Dynamic[T]) Set(v T) {
	registry.Dynamic.Set(val.KeyName, v)
}
