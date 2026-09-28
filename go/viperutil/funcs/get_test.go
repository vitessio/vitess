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

package funcs_test

import (
	"fmt"

	"github.com/spf13/viper"

	"vitess.io/vitess/go/viperutil"
	"vitess.io/vitess/go/viperutil/funcs"
	"vitess.io/vitess/go/viperutil/internal/value"
)

func ExampleGetPath() {
	v := viper.New()

	val := viperutil.Configure("path", viperutil.Options[[]string]{
		GetFunc: funcs.GetPath,
	})

	stub(val, v)

	v.Set(val.Key(), []string{"/var/www", "/usr:/usr/bin", "/vt"})
	fmt.Println(val.Get())
	// Output: [/var/www /usr /usr/bin /vt]
}

func stub[T any](val viperutil.Value[T], v *viper.Viper) {
	// N.B.: You cannot do this in normal code because these types are internal
	// to viperutil, but you also will not need to do this. However it's
	// necessary for the example to work here.
	base := val.(*value.Static[T]).Base
	base.BoundGetFunc = base.GetFunc(v)
}
