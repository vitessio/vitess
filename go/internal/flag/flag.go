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

// Package flag is an internal package to allow us to gracefully transition
// from the standard library's flag package to pflag. See VEP-4 for details.
//
// In general, this package should not be imported or depended on, except in the
// cases of package servenv, and entrypoints in go/cmd. This package WILL be
// deleted after the migration to pflag is completed, without any support for
// compatibility.
package flag

import (
	goflag "flag"
	"os"
	"reflect"
	"strings"

	"vitess.io/vitess/go/vt/log"
)

// Parse wraps the standard library's flag.Parse to perform some sanity checking
// and issue deprecation warnings in advance of our move to pflag.
//
// It also adjusts the global CommandLine's Usage func to print out flags with
// double-dashes when a user requests the help, attempting to otherwise leave
// the default Usage formatting unchanged.
//
// See VEP-4, phase 1 for details: https://github.com/vitessio/enhancements/blob/c766ea905e55409cddeb666d6073cd2ac4c9783e/veps/vep-4.md#phase-1-preparation
func Parse() {
	// First, override the Usage func to make flags show in their double-dash
	// forms to the user.
	SetUsage(goflag.CommandLine, UsageOptions{})

	// Then, parse as normal.
	goflag.Parse()

	// Finally, warn on deprecated flag usage.
	warnOnSingleDashLongFlags(goflag.CommandLine, os.Args, log.Warningf)
	warnOnMixedPositionalAndFlagArguments(goflag.Args(), log.Warningf)
}

// Args returns the positional arguments with the first double-dash ("--")
// removed. If no double-dash was specified on the command-line, this is
// equivalent to flag.Args() from the standard library flag package.
func Args() (args []string) {
	doubleDashIdx := -1
	for i, arg := range goflag.Args() {
		if arg == "--" {
			doubleDashIdx = i
			break
		}

		args = append(args, arg)
	}

	if doubleDashIdx != -1 {
		args = append(args, goflag.Args()[doubleDashIdx+1:]...)
	}

	return args
}

// Arg returns the ith command-line argument after flags have been processed,
// ignoring the first double-dash ("--") argument separator. If fewer than `i`
// arguments were specified, the empty string is returned. If no double-dash was
// specified, this is equivalent to flag.Arg(i) from the standard library flag
// package.
func Arg(i int) string {
	if args := Args(); len(args) > i {
		return args[i]
	}

	return ""
}

const (
	singleDashLongFlagsWarning  = "Use of single-dash long flags is deprecated and will be removed in the next version of Vitess. Please use --%s instead"
	mixedFlagsAndPosargsWarning = "Detected a dashed argument after a positional argument. " +
		"Currently these are treated as posargs that may be parsed by a subcommand, but in the next version of Vitess they will be parsed as top-level flags, which may not be defined, causing errors. " +
		"To preserve existing behavior, please update your invocation to include a \"--\" after all top-level flags to continue treating %s as a positional argument."
)

// Check and warn on any single-dash flags.
func warnOnSingleDashLongFlags(fs *goflag.FlagSet, argv []string, warningf func(msg string, args ...any)) {
	fs.Visit(func(f *goflag.Flag) {
		// Boolean flags with single-character names are okay to use the
		// single-dash form. I don't _think_ we have any of these, but I'm being
		// conservative here.
		if bf, ok := f.Value.(maybeBoolFlag); ok && bf.IsBoolFlag() && len(f.Name) == 1 {
			return
		}

		for _, arg := range argv {
			if strings.HasPrefix(arg, "-"+f.Name) {
				warningf(singleDashLongFlagsWarning, f.Name)
			}
		}
	})
}

// Check and warn for any mixed posarg / dashed-arg on the CLI.
func warnOnMixedPositionalAndFlagArguments(posargs []string, warningf func(msg string, args ...any)) {
	for _, arg := range posargs {
		if arg == "--" {
			break
		}

		if strings.HasPrefix(arg, "-") {
			log.Warningf(mixedFlagsAndPosargsWarning, arg)
		}
	}
}

// From the standard library documentation:
//	> If a Value has an IsBoolFlag() bool method returning true, the
//	> command-line parser makes -name equivalent to -name=true rather than
//	> using the next command-line argument.
//
// This also has less-well-documented implications for the default Usage
// behavior, which is why we are duplicating it.
type maybeBoolFlag interface {
	IsBoolFlag() bool
}

// isZeroValue determines whether the string represents the zero
// value for a flag.
// see https://cs.opensource.google/go/go/+/refs/tags/go1.17.7:src/flag/flag.go;l=451-465;drc=refs%2Ftags%2Fgo1.17.7
func isZeroValue(f *goflag.Flag, value string) bool {
	typ := reflect.TypeOf(f.Value)
	var z reflect.Value
	if typ.Kind() == reflect.Ptr {
		z = reflect.New(typ.Elem())
	} else {
		z = reflect.Zero(typ)
	}
	return value == z.Interface().(goflag.Value).String()
}
