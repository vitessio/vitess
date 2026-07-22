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

	flag "github.com/spf13/pflag"
)

// Parse wraps the standard library's flag.Parse to perform some sanity checking
// and issue deprecation warnings in advance of our move to pflag.
//
// It also adjusts the global CommandLine's Usage func to print out flags with
// double-dashes when a user requests the help, attempting to otherwise leave
// the default Usage formatting unchanged.
//
// See VEP-4, phase 1 for details: https://github.com/vitessio/enhancements/blob/c766ea905e55409cddeb666d6073cd2ac4c9783e/veps/vep-4.md#phase-1-preparation
func Parse(fs *flag.FlagSet) {
	fs.AddGoFlagSet(goflag.CommandLine)

	if fs.Lookup("help") == nil {
		var help bool

		if fs.ShorthandLookup("h") == nil {
			fs.BoolVarP(&help, "help", "h", false, "display usage and exit")
		} else {
			fs.BoolVar(&help, "help", false, "display usage and exit")
		}

		defer func() {
			if help {
				Usage()
				os.Exit(0)
			}
		}()
	}

	flag.CommandLine = fs
	flag.Parse()
}

// IsFlagProvided returns if the given flag has been provided by the user explicitly or not
func IsFlagProvided(name string) bool {
	fl := flag.Lookup(name)
	if fl != nil {
		return fl.Changed
	}
	return false
}

// Usage invokes the current CommandLine's Usage func, or if not overridden,
// "prints a simple header and calls PrintDefaults".
func Usage() {
	flag.Usage()
}

// filterTestFlags returns two slices: the second one has just the flags for `go test` and the first one contains
// the rest of the flags.
const (
	goTestFlagSuffix = "-test"
	goTestRunFlag    = "-test.run"
)

func filterTestFlags() ([]string, []string) {
	args := os.Args
	var testFlags []string
	var otherArgs []string
	hasExtraTestRunArg := false
	for i := 0; 0 < len(args) && i < len(args); i++ {
		// This additional logic to check for the test.run flag is required for running single unit tests in GoLand,
		// due to the way it uses "go tool test2json" to run the test. The CLI `go test` specifies the test as "-test.run=TestHeartbeat",
		// but test2json as "-test.run TestHeartbeat". So in the latter case we need to also add the arg following test.run
		if strings.HasPrefix(args[i], goTestFlagSuffix) || hasExtraTestRunArg {
			hasExtraTestRunArg = false
			testFlags = append(testFlags, args[i])
			if args[i] == goTestRunFlag {
				hasExtraTestRunArg = true
			}
			continue
		}
		otherArgs = append(otherArgs, args[i])
	}
	return otherArgs, testFlags
}

// ParseFlagsForTest parses `go test` flags separately from the app flags. The problem is that pflag.Parse() does not
// handle `go test` flags correctly. We need to separately parse the test flags using goflags. Additionally flags
// like test.Short() require that goflag.Parse() is called first.
func ParseFlagsForTest() {
	// We need to split up the test flags and the regular app pflags.
	// Then hand them off the std flags and pflags parsers respectively.
	args, testFlags := filterTestFlags()
	os.Args = args

	// Parse the testing flags
	if err := goflag.CommandLine.Parse(testFlags); err != nil {
		fmt.Println("Error parsing regular test flags:", err)
	}

	// Parse the remaining flags.
	flag.CommandLine.AddGoFlagSet(goflag.CommandLine)
	flag.Parse()
}

// Parsed returns true if the command-line flags have been parsed.
//
// It is agnostic to whether the standard library `flag` package or `pflag` was
// used for parsing, in order to facilitate the migration to `pflag` for
// VEP-4 [1].
//
// [1]: https://github.com/vitessio/vitess/issues/10697.
func Parsed() bool {
	return goflag.Parsed() || flag.Parsed()
}

// Lookup returns a pflag.Flag with the given name, from either the pflag or
// standard library `flag` CommandLine. If found in the latter, it is converted
// to a pflag.Flag first. If found in neither, this function returns nil.
func Lookup(name string) *flag.Flag {
	if f := flag.Lookup(name); f != nil {
		return f
	}

	if f := goflag.Lookup(name); f != nil {
		return flag.PFlagFromGoFlag(f)
	}

	return nil
}

// Args returns the positional arguments with the first double-dash ("--")
// removed. If no double-dash was specified on the command-line, this is
// equivalent to flag.Args() from the standard library flag package.
func Args() (args []string) {
	doubleDashIdx := -1
	for i, arg := range flag.Args() {
		if arg == "--" {
			doubleDashIdx = i
			break
		}

		args = append(args, arg)
	}

	if doubleDashIdx != -1 {
		args = append(args, flag.Args()[doubleDashIdx+1:]...)
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

// From the standard library documentation:
//
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
	if typ.Kind() == reflect.Pointer {
		z = reflect.New(typ.Elem())
	} else {
		z = reflect.Zero(typ)
	}
	return value == z.Interface().(goflag.Value).String()
}
