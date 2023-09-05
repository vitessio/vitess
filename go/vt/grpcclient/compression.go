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

package grpcclient

import (
	"fmt"
	"slices"

	"google.golang.org/grpc"
	"google.golang.org/grpc/encoding"
)

type compressionValue string

func (c *compressionValue) String() string {
	return string(*c)
}

func (c *compressionValue) Set(d string) error {
	if slices.Contains(compressionTypes, d) {
		*c = compressionValue(d)
	} else {
		return fmt.Errorf("unsupported compression type: %s", d)
	}
	return nil
}

func (c *compressionValue) Type() string {
	return "string"
}

var (
	compression      compressionValue
	compressionTypes []string
)

func appendCompression(opts []grpc.DialOption) ([]grpc.DialOption, error) {
	if compression != "" {
		dialOpt := grpc.WithDefaultCallOptions(grpc.UseCompressor(string(compression)))
		opts = append(opts, dialOpt)
	}

	return opts, nil
}

func init() {
	compressors := []encoding.Compressor{SnappyCompressor{}, ZstdCompressor{}}

	for i := range compressors {
		compressionTypes = append(compressionTypes, compressors[i].Name())
		encoding.RegisterCompressor(compressors[i])
	}

	RegisterGRPCDialOptions(appendCompression)
}
