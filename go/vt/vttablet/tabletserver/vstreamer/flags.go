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

package vstreamer

import (
	"time"
	"vitess.io/vitess/go/vt/servenv"

	"github.com/spf13/pflag"
)

var (
	vreplicationMaxExecutionTimeUnset = -1 * time.Millisecond
	vreplicationMaxExecutionTime      = vreplicationMaxExecutionTimeUnset
)

func registerVStreamerFlags(fs *pflag.FlagSet) {
	fs.DurationVar(&vreplicationMaxExecutionTime, "vreplication_max_execution_time", vreplicationMaxExecutionTimeUnset, "If set, controls the mysql MAX_EXCUTION_TIME query hint for vreplication queries (default -1, unset)")
}

func init() {
	servenv.OnParseFor("vtcombo", registerVStreamerFlags)
	servenv.OnParseFor("vttablet", registerVStreamerFlags)
}
