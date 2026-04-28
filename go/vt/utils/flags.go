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
	"time"

	"github.com/spf13/pflag"

	"vitess.io/vitess/go/flagutil"
)

/*
Contains utility functions for working with flags during the flags refactor project.
*/

// setFlagVar is a generic helper for registering flags.
// setFunc should be a function with signature func(fs *pflag.FlagSet, p *T, name string, def T, usage string)
func setFlagVar[T any](fs *pflag.FlagSet, p *T, name string, def T, usage string,
	setFunc func(fs *pflag.FlagSet, p *T, name string, def T, usage string),
) {
	if strings.Contains(name, "_") {
		fmt.Printf("[WARNING] Please use flag names with dashes instead of underscores, preparing for deprecation of underscores in flag names")
	}

	setFunc(fs, p, name, def, usage)
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

func SetFlagFloatDurationVar(fs *pflag.FlagSet, p *time.Duration, name string, def time.Duration, usage string) {
	setFlagVar(fs, p, name, def, usage, flagutil.FloatDuration)
}

func SetFlagUint32Var(fs *pflag.FlagSet, p *uint32, name string, def uint32, usage string) {
	setFlagVar(fs, p, name, def, usage, (*pflag.FlagSet).Uint32Var)
}

func SetFlagUint64Var(fs *pflag.FlagSet, p *uint64, name string, def uint64, usage string) {
	setFlagVar(fs, p, name, def, usage, (*pflag.FlagSet).Uint64Var)
}

func SetFlagStringSliceVar(fs *pflag.FlagSet, p *[]string, name string, def []string, usage string) {
	setFlagVar(fs, p, name, def, usage, (*pflag.FlagSet).StringSliceVar)
}

func SetFlagUintVar(fs *pflag.FlagSet, p *uint, name string, def uint, usage string) {
	setFlagVar(fs, p, name, def, usage, (*pflag.FlagSet).UintVar)
}

func SetFlagFloat64Var(fs *pflag.FlagSet, p *float64, name string, def float64, usage string) {
	setFlagVar(fs, p, name, def, usage, (*pflag.FlagSet).Float64Var)
}

// SetFlagVar registers a flag (that implements the pflag.Value interface).
func SetFlagVar(fs *pflag.FlagSet, value pflag.Value, name, usage string) {
	if strings.Contains(name, "_") {
		fmt.Printf("[WARNING] Please use flag names with dashes instead of underscores, preparing for deprecation of underscores in flag names")
	}
	fs.Var(value, name, usage)
}
