/*
Copyright 2023 The Vitess Authors.

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

package main

import (
	"regexp"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRegularExpressions(t *testing.T) {
	lists := []struct {
		regexp string
		input  string
		checkF func(t *testing.T, regexp *regexp.Regexp, input string)
	}{
		{
			regexp: regexpFindBootstrapVersion,
			input:  "BOOTSTRAP_VERSION=18.1",
			checkF: func(t *testing.T, regexp *regexp.Regexp, input string) {
				submatch := regexp.FindStringSubmatch(input)
				require.Len(t, submatch, 2, "Should have two submatches in the regular expression")
				require.Equal(t, "18.1", submatch[1])
			},
		},
		{
			regexp: regexpFindGolangVersion,
			input:  `goversion_min 1.20.5 || echo "Go version reported`,
			checkF: func(t *testing.T, regexp *regexp.Regexp, input string) {
				submatch := regexp.FindStringSubmatch(input)
				require.Len(t, submatch, 2, "Should have two submatches in the regular expression")
				require.Equal(t, "1.20.5", submatch[1])
			},
		},
		{
			regexp: regexpReplaceGoModGoVersion,
			input:  "go 1.20",
			checkF: func(t *testing.T, regexp *regexp.Regexp, input string) {
				res := regexp.ReplaceAllString(input, "go 1.21")
				require.Equal(t, "go 1.21", res)
			},
		},
		{
			regexp: regexpReplaceGoModGoVersion,
			input:  "go 1 20",
			checkF: func(t *testing.T, regexp *regexp.Regexp, input string) {
				res := regexp.ReplaceAllString(input, "go 1.21")
				require.Equal(t, "go 1 20", res)
			},
		},
		{
			regexp: regexpReplaceDockerfileBootstrapVersion,
			input:  "ARG bootstrap_version=18.1",
			checkF: func(t *testing.T, regexp *regexp.Regexp, input string) {
				res := regexp.ReplaceAllString(input, "ARG bootstrap_version=18.2")
				require.Equal(t, "ARG bootstrap_version=18.2", res)
			},
		},
		{
			regexp: regexpReplaceMakefileBootstrapVersion,
			input:  "BOOTSTRAP_VERSION=18.1",
			checkF: func(t *testing.T, regexp *regexp.Regexp, input string) {
				res := regexp.ReplaceAllString(input, "BOOTSTRAP_VERSION=18.2")
				require.Equal(t, "BOOTSTRAP_VERSION=18.2", res)
			},
		},
		{
			regexp: regexpReplaceTestGoBootstrapVersion,
			input:  `flag.String("bootstrap-version", "18.1", "the version identifier to use for the docker images")`,
			checkF: func(t *testing.T, regexp *regexp.Regexp, input string) {
				res := regexp.ReplaceAllString(input, "\"bootstrap-version\", \"18.2\"")
				require.Equal(t, `flag.String("bootstrap-version", "18.2", "the version identifier to use for the docker images")`, res)
			},
		},
		{
			regexp: regexpReplaceGolangVersionInWorkflow,
			input:  "go-version: 1.20.5",
			checkF: func(t *testing.T, regexp *regexp.Regexp, input string) {
				res := regexp.ReplaceAllString(input, "go-version: 1.20.6")
				require.Equal(t, `go-version: 1.20.6`, res)
			},
		},
	}

	for _, list := range lists {
		t.Run(list.regexp+" "+list.input, func(t *testing.T) {
			list.checkF(t, regexp.MustCompile(list.regexp), list.input)
		})
	}
}
