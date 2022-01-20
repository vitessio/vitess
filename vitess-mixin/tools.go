//go:build tools
// +build tools

package main

import (
	_ "github.com/google/go-jsonnet/cmd/jsonnet"
	_ "github.com/google/go-jsonnet/cmd/jsonnetfmt"
	_ "github.com/jsonnet-bundler/jsonnet-bundler/cmd/jb"
	_ "github.com/prometheus/prometheus/cmd/promtool"
)
