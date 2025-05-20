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
	"math/rand/v2"
	"strings"
	"time"

	"github.com/spf13/pflag"
	// "vitess.io/vitess/go/vt/log"
)

/*
Contains utility functions for working with flags during the flags refactor project.
*/

// flagVariants returns two variants of the flag name:
// one with dashes replaced by underscores and one with underscores replaced by dashes.
func flagVariants(name string) (underscored, dashed string) {
	prefix := "--"
	if strings.HasPrefix(name, prefix) {
		nameWithoutPrefix := strings.TrimPrefix(name, prefix)
		underscored = prefix + strings.ReplaceAll(nameWithoutPrefix, "-", "_")
		dashed = prefix + strings.ReplaceAll(nameWithoutPrefix, "_", "-")
	} else {
		underscored = strings.ReplaceAll(name, "-", "_")
		dashed = strings.ReplaceAll(name, "_", "-")
	}
	return
}

// setFlagVar is a generic helper for registering flags.
// setFunc should be a function with signature func(fs *pflag.FlagSet, p *T, name string, def T, usage string)
func setFlagVar[T any](fs *pflag.FlagSet, p *T, name string, def T, usage string,
	setFunc func(fs *pflag.FlagSet, p *T, name string, def T, usage string)) {

	underscored, dashed := flagVariants(name)
	if name == underscored {
		fmt.Printf("[WARNING] Please use flag names with dashes instead of underscores, preparing for deprecation of underscores in flag names")
	}

	setFunc(fs, p, dashed, def, usage)
	setFunc(fs, p, underscored, def, "")
	_ = fs.MarkHidden(underscored)
	_ = fs.MarkDeprecated(underscored, fmt.Sprintf("use %s instead", dashed))
}

func SetFlagIntVar(fs *pflag.FlagSet, p *int, name string, def int, usage string) {
	setFlagVar(fs, p, name, def, usage, (*pflag.FlagSet).IntVar)
}

func SetFlagInt64Var(fs *pflag.FlagSet, p *int64, name string, def int64, usage string) {
	setFlagVar(fs, p, name, def, usage, (*pflag.FlagSet).Int64Var)
}

func SetFlagBoolVar(fs *pflag.FlagSet, p *bool, name string, def bool, usage string) {
	setFlagVar(fs, p, name, def, usage, (*pflag.FlagSet).BoolVar)
}

func SetFlagStringVar(fs *pflag.FlagSet, p *string, name string, def string, usage string) {
	setFlagVar(fs, p, name, def, usage, (*pflag.FlagSet).StringVar)
}

func SetFlagDurationVar(fs *pflag.FlagSet, p *time.Duration, name string, def time.Duration, usage string) {
	setFlagVar(fs, p, name, def, usage, (*pflag.FlagSet).DurationVar)
}

func SetFlagUint64Var(fs *pflag.FlagSet, p *uint64, name string, def uint64, usage string) {
	setFlagVar(fs, p, name, def, usage, (*pflag.FlagSet).Uint64Var)
}

func SetFlagStringSliceVar(fs *pflag.FlagSet, p *[]string, name string, def []string, usage string) {
	setFlagVar(fs, p, name, def, usage, (*pflag.FlagSet).StringSliceVar)
}

// SetFlagVar registers a flag (that implements the pflag.Value interface)
// using both the dashed and underscored versions of the flag name.
// The underscored version is hidden and marked as deprecated.
func SetFlagVar(fs *pflag.FlagSet, value pflag.Value, name, usage string) {
	underscored, dashed := flagVariants(name)
	if name == underscored {
		fmt.Printf("[WARNING] Please use flag names with dashes instead of underscores, preparing for deprecation of underscores in flag names")
	}
	fs.Var(value, dashed, usage)
	fs.Var(value, underscored, "")
	_ = fs.MarkHidden(underscored)
	_ = fs.MarkDeprecated(underscored, fmt.Sprintf("use %s instead", dashed))
}

// SetFlagVariantsForTests randomly assigns either the underscored or dashed version of the flag name to the map.
// This is designed to help catch cases where code does not properly handle both formats during testing.
func SetFlagVariantsForTests(m map[string]string, key, value string) {
	underscored, dashed := flagVariants(key)
	if rand.Int()%2 == 0 {
		m[underscored] = value
	} else {
		m[dashed] = value
	}
}

// GetFlagVariantForTests randomly returns either the underscored or dashed version of the flag name.
func GetFlagVariantForTests(flagName string) string {
	underscored, dashed := flagVariants(flagName)
	if rand.Int()%2 == 0 {
		// fmt.Print("Using flag variant: ", underscored, "\n")
		return underscored
	}
	// fmt.Print("Using flag variant: ", dashed, "\n")
	return dashed
}
