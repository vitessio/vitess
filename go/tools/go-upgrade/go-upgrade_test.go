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
	"os"
	"path/filepath"
	"regexp"
	"testing"

	"github.com/hashicorp/go-version"
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
			regexp: regexpFindGoModGoVersion,
			input:  "module vitess.io/vitess\n\ngo 1.20.5\n\nrequire golang.org/x/tools v0.1.0\n",
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
			regexp: regexpReplaceGolangDockerImage,
			input:  "FROM --platform=linux/amd64 golang:1.25.3-bookworm@sha256:414a753c2f67d0efccb01b5f58b3d3a8a2cbb7c012ce9e535418b5b3492b2c24 AS builder",
			checkF: func(t *testing.T, regexp *regexp.Regexp, input string) {
				res := regexp.ReplaceAllString(input, "${1}1.25.4-${2}@sha256:1111111111111111111111111111111111111111111111111111111111111111")
				require.Equal(t, "FROM --platform=linux/amd64 golang:1.25.4-bookworm@sha256:1111111111111111111111111111111111111111111111111111111111111111 AS builder", res)
			},
		},
		{
			regexp: regexpReplaceGolangDockerImage,
			input:  "FROM --platform=linux/arm64 golang:1.25.3-bookworm@sha256:414a753c2f67d0efccb01b5f58b3d3a8a2cbb7c012ce9e535418b5b3492b2c24 AS builder",
			checkF: func(t *testing.T, regexp *regexp.Regexp, input string) {
				res := regexp.ReplaceAllString(input, "${1}1.25.4-${2}@sha256:1111111111111111111111111111111111111111111111111111111111111111")
				require.Equal(t, input, res)
			},
		},
		{
			regexp: regexpReplaceGolangDockerImage,
			input:  "FROM golang:1.25.3-trixie@sha256:414a753c2f67d0efccb01b5f58b3d3a8a2cbb7c012ce9e535418b5b3492b2c24 AS builder",
			checkF: func(t *testing.T, regexp *regexp.Regexp, input string) {
				res := regexp.ReplaceAllString(input, "${1}1.25.4-${2}@sha256:1111111111111111111111111111111111111111111111111111111111111111")
				require.Equal(t, "FROM golang:1.25.4-trixie@sha256:1111111111111111111111111111111111111111111111111111111111111111 AS builder", res)
			},
		},
		{
			regexp: regexpReplaceGolangDockerImage,
			input:  "ARG image=golang:1.25.3-bookworm@sha256:414a753c2f67d0efccb01b5f58b3d3a8a2cbb7c012ce9e535418b5b3492b2c24",
			checkF: func(t *testing.T, regexp *regexp.Regexp, input string) {
				res := regexp.ReplaceAllString(input, "${1}1.25.4-${2}@sha256:1111111111111111111111111111111111111111111111111111111111111111")
				require.Equal(t, "ARG image=golang:1.25.4-bookworm@sha256:1111111111111111111111111111111111111111111111111111111111111111", res)
			},
		},
	}

	for _, list := range lists {
		t.Run(list.regexp+" "+list.input, func(t *testing.T) {
			list.checkF(t, regexp.MustCompile(list.regexp), list.input)
		})
	}
}

func TestGoModFilesToUpgrade(t *testing.T) {
	dir := t.TempDir()

	require.NoError(t, os.WriteFile(filepath.Join(dir, "go.mod"), []byte("module example\n\ngo 1.26.3\n"), 0o644))

	for _, tool := range []string{"goyacc", "gofumpt"} {
		toolDir := filepath.Join(dir, "tools", tool)
		require.NoError(t, os.MkdirAll(toolDir, 0o755))
		require.NoError(t, os.WriteFile(filepath.Join(toolDir, "go.mod"), []byte("module example/"+tool+"\n\ngo 1.26.3\n"), 0o644))
	}

	// A tool directory without a go.mod must not be picked up.
	require.NoError(t, os.MkdirAll(filepath.Join(dir, "tools", "notamodule"), 0o755))

	origWd, err := os.Getwd()
	require.NoError(t, err)
	require.NoError(t, os.Chdir(dir))
	t.Cleanup(func() {
		require.NoError(t, os.Chdir(origWd))
	})

	files, err := goModFilesToUpgrade()
	require.NoError(t, err)

	require.Len(t, files, 3)
	require.Contains(t, files, "./go.mod")

	toolModules := map[string]bool{}
	for _, file := range files {
		toolModules[filepath.Base(filepath.Dir(file))] = true
	}
	require.True(t, toolModules["goyacc"])
	require.True(t, toolModules["gofumpt"])
}

func TestCurrentGolangVersion(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "go.mod"),
		[]byte("module vitess.io/vitess\n\ngo 1.26.4\n\nrequire golang.org/x/tools v0.1.0\n"), 0o644))

	origWd, err := os.Getwd()
	require.NoError(t, err)
	require.NoError(t, os.Chdir(dir))
	t.Cleanup(func() {
		require.NoError(t, os.Chdir(origWd))
	})

	v, err := currentGolangVersion()
	require.NoError(t, err)
	require.Equal(t, "1.26.4", v.String())
}

func TestUpgradeGoModFiles(t *testing.T) {
	dir := t.TempDir()

	rootGoMod := filepath.Join(dir, "go.mod")
	require.NoError(t, os.WriteFile(rootGoMod, []byte("module vitess.io/vitess\n\ngo 1.26.3\n"), 0o644))

	// A tool module that has drifted behind the root module.
	driftedTool := filepath.Join(dir, "tools", "goyacc", "go.mod")
	require.NoError(t, os.MkdirAll(filepath.Dir(driftedTool), 0o755))
	require.NoError(t, os.WriteFile(driftedTool, []byte("module vitess.io/vitess/go/tools/goyacc\n\ngo 1.26.1\n"), 0o644))

	// A tool module already at the target version: it must be left byte-identical.
	currentTool := filepath.Join(dir, "tools", "gofumpt", "go.mod")
	require.NoError(t, os.MkdirAll(filepath.Dir(currentTool), 0o755))
	currentContent := []byte("module vitess.io/vitess/go/tools/gofumpt\n\ngo 1.26.4\n")
	require.NoError(t, os.WriteFile(currentTool, currentContent, 0o644))

	origWd, err := os.Getwd()
	require.NoError(t, err)
	require.NoError(t, os.Chdir(dir))
	t.Cleanup(func() {
		require.NoError(t, os.Chdir(origWd))
	})

	target, err := version.NewVersion("1.26.4")
	require.NoError(t, err)
	require.NoError(t, upgradeGoModFiles(target))

	for _, file := range []string{rootGoMod, driftedTool, currentTool} {
		content, err := os.ReadFile(file)
		require.NoError(t, err)
		require.Contains(t, string(content), "go 1.26.4")
	}

	// The already-current module must be untouched, byte-for-byte.
	got, err := os.ReadFile(currentTool)
	require.NoError(t, err)
	require.Equal(t, currentContent, got)
}
