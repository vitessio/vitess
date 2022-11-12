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
	"github.com/spf13/pflag"

	"vitess.io/vitess/go/viperutil/v2/internal/value"
)

var (
	_ Value[int] = (*value.Static[int])(nil)
	_ Value[int] = (*value.Dynamic[int])(nil)
)

type Value[T any] interface {
	value.Registerable
	Get() T
	Default() T
}

func BindFlags(fs *pflag.FlagSet, values ...value.Registerable) {
	value.BindFlags(fs, values...)
}
