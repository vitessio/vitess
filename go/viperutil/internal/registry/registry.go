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

package registry

import (
	"github.com/spf13/pflag"
	"github.com/spf13/viper"

	"vitess.io/vitess/go/viperutil/internal/sync"
)

var (
	// Static is the registry for static config variables. These variables will
	// never be affected by a Watch-ed config, and maintain their original
	// values for the lifetime of the process.
	Static = viper.New()
	// Dynamic is the registry for dynamic config variables. If a config file is
	// found by viper, it will be watched by a threadsafe wrapper around a
	// second viper (see sync.Viper), and variables registered to it will pick
	// up changes to that config file throughout the lifetime of the process.
	Dynamic = sync.New()

	_ Bindable = (*viper.Viper)(nil)
	_ Bindable = (*sync.Viper)(nil)
)

// Bindable represents the methods needed to bind a value.Value to a given
// registry. It exists primarly to allow us to treat a sync.Viper as a
// viper.Viper for configuration registration purposes.
type Bindable interface {
	BindEnv(vars ...string) error
	BindPFlag(key string, flag *pflag.Flag) error
	RegisterAlias(alias string, key string)
	SetDefault(key string, value any)
}

// Combined returns a viper combining the Static and Dynamic registries.
func Combined() *viper.Viper {
	v := viper.New()
	_ = v.MergeConfigMap(Static.AllSettings())
	_ = v.MergeConfigMap(Dynamic.AllSettings())

	v.SetConfigFile(Static.ConfigFileUsed())
	return v
}
