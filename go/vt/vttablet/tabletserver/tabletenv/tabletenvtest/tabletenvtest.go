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

package tabletenvtest

import (
	"github.com/spf13/pflag"

	_flag "vitess.io/vitess/go/internal/flag"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/tabletenv"
)

// LoadTabletEnvFlags loads the default values for the tabletenv flags and is useful for tests which
// expect these defaults to be setup.
func LoadTabletEnvFlags() {
	_flag.ParseFlagsForTest()
	fs := pflag.NewFlagSet("TestFlags", pflag.ContinueOnError)
	tabletenv.RegisterTabletEnvFlags(fs)
	pflag.Parse()
}
