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
	"net/http/httptest"
	"net/url"
	"regexp"
	"testing"

	"github.com/google/go-containerregistry/pkg/name"
	"github.com/google/go-containerregistry/pkg/registry"
	gocr "github.com/google/go-containerregistry/pkg/v1"
	"github.com/google/go-containerregistry/pkg/v1/empty"
	"github.com/google/go-containerregistry/pkg/v1/mutate"
	"github.com/google/go-containerregistry/pkg/v1/random"
	"github.com/google/go-containerregistry/pkg/v1/remote"
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

// TestResolveGolangImageDigestPinsMultiPlatformIndex publishes a golang tag whose manifest is a
// multi-platform index and checks that the resolver pins the index itself rather than one of its
// per-platform children. A per-platform pin cannot be built natively on any other architecture.
func TestResolveGolangImageDigestPinsMultiPlatformIndex(t *testing.T) {
	srv := httptest.NewServer(registry.New())
	t.Cleanup(srv.Close)
	registryURL, err := url.Parse(srv.URL)
	require.NoError(t, err)

	var idx gocr.ImageIndex = empty.Index
	for _, arch := range []string{"amd64", "arm64"} {
		img, err := random.Image(256, 1)
		require.NoError(t, err)
		idx = mutate.AppendManifests(idx, mutate.IndexAddendum{
			Add:        img,
			Descriptor: gocr.Descriptor{Platform: &gocr.Platform{OS: "linux", Architecture: arch}},
		})
	}

	goVersion, err := version.NewVersion("1.27.1")
	require.NoError(t, err)
	repository := registryURL.Host + "/golang"
	ref, err := name.ParseReference(repository + ":" + golangDockerTag(goVersion, "bookworm"))
	require.NoError(t, err)
	require.NoError(t, remote.WriteIndex(ref, idx))

	want, err := idx.Digest()
	require.NoError(t, err)

	got, err := resolveGolangImageDigest(repository, goVersion, "bookworm")
	require.NoError(t, err)
	require.Equal(t, want.String(), got)
}
