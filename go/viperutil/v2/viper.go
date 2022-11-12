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

package viperutil

import (
	"strings"

	"github.com/spf13/viper"

	"vitess.io/vitess/go/viperutil/v2/internal/value"
)

type Options[T any] struct {
	Aliases  []string
	FlagName string
	EnvVars  []string
	Default  T

	Dynamic bool

	GetFunc func(v *viper.Viper) func(key string) T
}

func Configure[T any](key string, opts Options[T]) (v Value[T]) {
	getfunc := opts.GetFunc
	if getfunc == nil {
		getfunc = GetFuncForType[T]()
	}

	base := &value.Base[T]{
		KeyName:    key,
		DefaultVal: opts.Default,
		GetFunc:    getfunc,
		Aliases:    opts.Aliases,
		FlagName:   opts.FlagName,
		EnvVars:    opts.EnvVars,
	}

	switch {
	case opts.Dynamic:
		v = value.NewDynamic(base)
	default:
		v = value.NewStatic(base)
	}

	return v
}

func KeyPrefixFunc(prefix string) func(subkey string) (fullkey string) {
	var keyParts []string
	if prefix != "" {
		keyParts = append(keyParts, prefix)
	}

	return func(subkey string) (fullkey string) {
		tmp := keyParts
		if subkey != "" {
			tmp = append(tmp, subkey)
		}

		return strings.Join(tmp, ".")
	}
}
