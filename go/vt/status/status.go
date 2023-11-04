/*
Copyright 2019 The Vitess Authors.

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

// Package status defines a few useful functions for our binaries,
// mainly to link the status page with a vtctld instance.
package status

import (
	"github.com/spf13/pflag"

	"vitess.io/vitess/go/vt/servenv"
)

// TODO(deepthi): This entire file (and package) can be deleted after v17
func registerFlags(fs *pflag.FlagSet) {
	fs.String("vtctld_addr", "", "address of a vtctld instance")
	_ = fs.MarkDeprecated("vtctld_addr", "will be removed after v17")
}

func init() {
	servenv.OnParseFor("vtcombo", registerFlags)
	servenv.OnParseFor("vtgate", registerFlags)
	servenv.OnParseFor("vttablet", registerFlags)
}
