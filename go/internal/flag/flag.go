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
	"fmt"
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
	goflag.CommandLine.Usage = func() {
		fmt.Fprintf(goflag.CommandLine.Output(), "Usage of %s:\n", os.Args[0])

		var buf strings.Builder
		goflag.CommandLine.VisitAll(func(f *goflag.Flag) {
			defer buf.Reset()
			defer func() { fmt.Fprintf(goflag.CommandLine.Output(), "%s\n", buf.String()) }()

			// See https://cs.opensource.google/go/go/+/refs/tags/go1.17.7:src/flag/flag.go;l=512;drc=refs%2Ftags%2Fgo1.17.7
			// for why two leading spaces.
			buf.WriteString("  ")

			// We use `UnquoteUsage` to preserve the "name override"
			// behavior of the standard flag package, documented here:
			//
			//	> The listed type, here int, can be changed by placing a
			//	> back-quoted name in the flag's usage string; the first
			//	> such item in the message is taken to be a parameter name
			//	> to show in the message and the back quotes are stripped
			//	> from the message when displayed. For instance, given
			//	>
			//	>	flag.String("I", "", "search `directory` for include files")
			//	>
			//	> the output will be
			//	>
			// 	> 	-I directory
			//	>		search directory for include files.
			name, usage := goflag.UnquoteUsage(f)

			// From the standard library documentation:
			//	> For bool flags, the type is omitted and if the flag name is
			//	> one byte the usage message appears on the same line.
			if bf, ok := f.Value.(maybeBoolFlag); ok && bf.IsBoolFlag() && len(name) == 1 {
				fmt.Fprintf(&buf, "-%s\t%s", f.Name, usage)
				return
			}

			// First line: name, and, type or backticked name.
			buf.WriteString("--")
			buf.WriteString(f.Name)
			if name != "" {
				fmt.Fprintf(&buf, " %s", name)
			}
			buf.WriteString("\n\t")

			// Second line: usage and optional default, if not the zero value
			// for the type.
			buf.WriteString(usage)
			if !isZeroValue(f, f.DefValue) {
				fmt.Fprintf(&buf, " (default %s)", f.DefValue)
			}
		})
	}

	goflag.Parse()

	// Check and warn on any single-dash flags.
	argv := os.Args
	goflag.Visit(func(f *goflag.Flag) {
		// Boolean flags with single-character names are okay to use the
		// single-dash form. I don't _think_ we have any of these, but I'm being
		// conservative here.
		if bf, ok := f.Value.(maybeBoolFlag); ok && bf.IsBoolFlag() && len(f.Name) == 1 {
			return
		}

		for _, arg := range argv {
			if strings.HasPrefix(arg, "-"+f.Name) {
				log.Warningf("Use of single-dash long flags is deprecated and will be removed in the next version of Vitess. Please use --%s instead", f.Name)
			}
		}
	})

	// Check and warn for any mixed posarg / dashed-arg on the CLI.
	posargs := goflag.Args()
	for _, arg := range posargs {
		if arg == "--" {
			break
		}

		if strings.HasPrefix(arg, "-") {
			log.Warningf("Detected a positional argument beginning with a dash; This will be treated as a flag argument in the next version of Vitess. Please update your invocation to include a \"--\" before to continue treating %s as a positional argument.", arg)
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
