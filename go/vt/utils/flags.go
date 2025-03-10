/*
Copyright 2025 The Vitess Authors.

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

package utils

import (
	"fmt"
	"strings"

	"github.com/spf13/pflag"

	"vitess.io/vitess/go/vt/log"
)

/*
Contains utility functions for working with flags during the flags refactor project.
*/

// flagVariants returns two variants of the flag name:
// one with dashes replaced by underscores and one with underscores replaced by dashes.
func flagVariants(name string) (underscored, dashed string) {
	underscored = strings.ReplaceAll(name, "-", "_")
	dashed = strings.ReplaceAll(name, "_", "-")
	return
}

// setFlagVar is a generic helper for registering flags.
// setFunc should be a function with signature func(fs *pflag.FlagSet, p *T, name string, def T, usage string)
func setFlagVar[T any](fs *pflag.FlagSet, p *T, name string, def T, usage string,
	setFunc func(fs *pflag.FlagSet, p *T, name string, def T, usage string)) {

	underscored, dashed := flagVariants(name)
	if name == underscored {
		log.Warning("Please use flag names with dashes instead of underscores, preparing for deprecation of underscores in flag names")
	}
	setFunc(fs, p, dashed, def, usage)
	setFunc(fs, p, underscored, def, "")
	_ = fs.MarkHidden(underscored)
	_ = fs.MarkDeprecated(underscored, fmt.Sprintf("use %s instead", dashed))
}

func SetFlagIntVar(fs *pflag.FlagSet, p *int, name string, def int, usage string) {
	setFlagVar(fs, p, name, def, usage, (*pflag.FlagSet).IntVar)
}

func SetFlagBoolVar(fs *pflag.FlagSet, p *bool, name string, def bool, usage string) {
	setFlagVar(fs, p, name, def, usage, (*pflag.FlagSet).BoolVar)
}

func SetFlagVariants(m map[string]string, key, value string) {
	underscored, dashed := flagVariants(key)
	m[underscored] = value
	m[dashed] = value
}
