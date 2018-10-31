/*
Copyright 2017 Google Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreedto in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package logutil

import (
	"flag"
)

func init() {
	threshold := flag.Lookup("stderrthreshold")
	if threshold == nil {
		// the logging module doesn't specify a stderrthreshold flag
		return
	}

	const warningLevel = "1"
	if err := threshold.Value.Set(warningLevel); err != nil {
		return
	}
	threshold.DefValue = warningLevel
}
