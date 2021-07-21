//+build tools

package tools

// Package tools is used to import go modules that we use for tooling as dependencies.
// For more information, please refer to: https://github.com/go-modules-by-example/index/blob/master/010_tools/README.md

import (
	_ "github.com/gogo/protobuf/protoc-gen-gofast"
	_ "k8s.io/code-generator/cmd/client-gen"
	_ "k8s.io/code-generator/cmd/deepcopy-gen"
	_ "k8s.io/code-generator/cmd/informer-gen"
	_ "k8s.io/code-generator/cmd/lister-gen"
)
