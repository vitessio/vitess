/*
Copyright 2026 The Vitess Authors.

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

package prefix

import (
	"fmt"

	"example.com/consts"
	"vitess.io/vitess/go/vt/log"
)

const component = "vstreamer"

type code string

const codeConst code = "component"

func prefixes(name string) {
	log.NewPrefixedLogger("constant")
	log.NewPrefixedLogger(component)
	log.NewPrefixedLogger(component + ":")
	log.NewPrefixedLogger(("wrapped"))
	log.NewPrefixedLogger(string(codeConst))
	log.NewPrefixedLogger(consts.Prefix)
	log.NewPrefixedLogger(name)                        // want "log prefix must be a constant; pass values as slog attributes"
	log.NewPrefixedLogger(component + name)            // want "log prefix must be a constant; pass values as slog attributes"
	log.NewPrefixedLogger(fmt.Sprintf("bad %s", name)) // want "log prefix must be a constant; pass values as slog attributes"
}
