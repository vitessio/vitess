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
package flag

import (
	goflag "flag"
	"fmt"
	"io"
	"os"
	"strings"
)

// UsageOptions controls the custom behavior when overriding the Usage for a
// FlagSet.
type UsageOptions struct {
	// Preface determines the beginning of the help text, before flag usages
	// and defaults. If this function is nil, the Usage will print "Usage of <os.Args[0]:\n".
	Preface func(w io.Writer)
	// Epilogue optionally prints text after the flag usages and defaults. If
	// this function is nil, the flag usage/defaults will be the end of the
	// Usage text.
	Epilogue func(w io.Writer)
	// FlagFilter allows certain flags to be omitted from the flag usage and
	// defaults. If non-nil, flags for which this function returns false are
	// omitted.
	FlagFilter func(f *goflag.Flag) bool
}

// SetUsage sets the Usage function for the given FlagSet according to the
// options. For VEP-4, all flags are printed in their double-dash form.
func SetUsage(fs *goflag.FlagSet, opts UsageOptions) {
	flagFilter := opts.FlagFilter
	if flagFilter == nil {
		flagFilter = func(f *goflag.Flag) bool { return true }
	}

	fs.Usage = func() {
		switch opts.Preface {
		case nil:
			fmt.Fprintf(fs.Output(), "Usage of %s:\n", os.Args[0])
		default:
			opts.Preface(fs.Output())
		}

		var buf strings.Builder
		fs.VisitAll(func(f *goflag.Flag) {
			if !flagFilter(f) {
				return
			}

			defer buf.Reset()
			defer func() { fmt.Fprintf(fs.Output(), "%s\n", buf.String()) }()

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

		if opts.Epilogue != nil {
			opts.Epilogue(fs.Output())
		}
	}
}
