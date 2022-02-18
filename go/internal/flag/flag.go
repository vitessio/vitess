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
	"strings"

	"vitess.io/vitess/go/vt/log"
)

// Parse wraps the standard library's flag.Parse to perform some sanity checking
// and issue deprecation warnings in advance of our move to pflag.
//
// See VEP-4, phase 1 for details: https://github.com/vitessio/enhancements/blob/c766ea905e55409cddeb666d6073cd2ac4c9783e/veps/vep-4.md#phase-1-preparation
func Parse() {
	goflag.Parse()

	// Check and warn on any single-dash flags.
	argv := os.Args
	goflag.Visit(func(f *goflag.Flag) {
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
