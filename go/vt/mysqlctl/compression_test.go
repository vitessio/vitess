/*
Copyright 2021 The Vitess Authors.

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

package mysqlctl

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/logutil"
)

func TestGetExtensionFromEngine(t *testing.T) {
	tests := []struct {
		engine, extension string
		err               error
	}{
		{"pgzip", ".gz", nil},
		{"pargzip", ".gz", nil},
		{"lz4", ".lz4", nil},
		{"zstd", ".zst", nil},
		{"foobar", "", errUnsupportedCompressionEngine},
	}

	for _, tt := range tests {
		t.Run(tt.engine, func(t *testing.T) {
			ext, err := getExtensionFromEngine(tt.engine)
			// if err != tt.err {
			if !errors.Is(err, tt.err) {
				t.Errorf("got err: %v; expected: %v", err, tt.err)
			}
			// }

			if ext != tt.extension {
				t.Errorf("got err: %v; expected: %v", ext, tt.extension)
			}
		})
	}
}

func TestBuiltinCompressors(t *testing.T) {
	data := []byte("foo bar foobar")
	logger := logutil.NewMemoryLogger()

	for _, engine := range []string{"pgzip", "lz4", "zstd"} {
		t.Run(engine, func(t *testing.T) {
			var compressed, decompressed bytes.Buffer
			reader := bytes.NewReader(data)
			compressor, err := newBuiltinCompressor(engine, &compressed, logger)
			if err != nil {
				t.Fatal(err)
			}
			_, err = io.Copy(compressor, reader)
			if err != nil {
				t.Error(err)
				return
			}
			compressor.Close()
			decompressor, err := newBuiltinDecompressor(engine, &compressed, logger)
			if err != nil {
				t.Error(err)
				return
			}
			_, err = io.Copy(&decompressed, decompressor)
			if err != nil {
				t.Error(err)
				return
			}
			decompressor.Close()
			if len(data) != len(decompressed.Bytes()) {
				t.Errorf("Different size of original (%d bytes) and uncompressed (%d bytes) data", len(data), len(decompressed.Bytes()))
			}

			if !reflect.DeepEqual(data, decompressed.Bytes()) {
				t.Error("decompressed content differs from the original")
			}
		})
	}
}

func TestUnSupportedBuiltinCompressors(t *testing.T) {
	logger := logutil.NewMemoryLogger()

	for _, engine := range []string{"external", "foobar"} {
		t.Run(engine, func(t *testing.T) {
			_, err := newBuiltinCompressor(engine, nil, logger)
			require.ErrorContains(t, err, "unsupported engine value for --compression-engine-name. supported values are 'external', 'pgzip', 'pargzip', 'zstd', 'lz4' value:")
		})
	}
}

func TestExternalCompressors(t *testing.T) {
	data := []byte("foo bar foobar")
	logger := logutil.NewMemoryLogger()

	tests := []struct {
		compress, decompress string
	}{
		{"gzip", "gzip -d"},
		{"pigz", "pigz -d"},
		{"lz4", "lz4 -d"},
		{"zstd", "zstd -d"},
		{"lzop", "lzop -d"},
		{"bzip2", "bzip2 -d"},
		{"lzma", "lzma -d"},
	}

	for _, tt := range tests {
		t.Run(tt.compress, func(t *testing.T) {
			var compressed, decompressed bytes.Buffer
			reader := bytes.NewReader(data)
			for _, cmd := range []string{tt.compress, tt.decompress} {
				cmdArgs := strings.Split(cmd, " ")

				_, err := validateExternalCmd(cmdArgs[0])
				if err != nil {
					t.Skip("Command not available in this host:", err)
				}
			}
			ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
			defer cancel()
			compressor, err := newExternalCompressor(ctx, tt.compress, &compressed, logger)
			if err != nil {
				t.Error(err)
				return
			}
			_, err = io.Copy(compressor, reader)
			if err != nil {
				t.Error(err)
				return
			}
			compressor.Close()
			decompressor, err := newExternalDecompressor(ctx, tt.decompress, &compressed, logger)
			if err != nil {
				t.Error(err)
				return
			}
			_, err = io.Copy(&decompressed, decompressor)
			if err != nil {
				t.Error(err)
				return
			}
			decompressor.Close()
			if len(data) != len(decompressed.Bytes()) {
				t.Errorf("Different size of original (%d bytes) and uncompressed (%d bytes) data", len(data), len(decompressed.Bytes()))
			}
			if !reflect.DeepEqual(data, decompressed.Bytes()) {
				t.Error("decompressed content differs from the original")
			}

		})
	}
}

func TestValidateExternalCmd(t *testing.T) {
	tests := []struct {
		cmdName string
		path    string
		errStr  string
	}{
		// this should not find an executable
		{"non_existent_cmd", "", "executable file not found"},
		// we expect ls to be on PATH as it is a basic command part of busybox and most containers
		{"ls", "ls", ""},
	}

	for i, tt := range tests {
		t.Run(fmt.Sprintf("Test #%d", i+1), func(t *testing.T) {
			CmdName := tt.cmdName
			path, err := validateExternalCmd(CmdName)
			if tt.path != "" {
				if !strings.HasSuffix(path, tt.path) {
					t.Errorf("Expected path \"%s\" to include \"%s\"", path, tt.path)
				}
			}
			if tt.errStr == "" {
				if err != nil {
					t.Errorf("Expected result \"%v\", got \"%v\"", "<nil>", err)
				}
			} else {
				if !strings.Contains(fmt.Sprintf("%v", err), tt.errStr) {
					t.Errorf("Expected result \"%v\", got \"%v\"", tt.errStr, err)
				}
			}
		})
	}
}

func TestValidateCompressionEngineName(t *testing.T) {
	tests := []struct {
		engineName string
		errStr     string
	}{
		// we expect ls to be on PATH as it is a basic command part of busybox and most containers
		{"external", ""},
		{"foobar", "unsupported engine value for --compression-engine-name. supported values are 'external', 'pgzip', 'pargzip', 'zstd', 'lz4' value: \"foobar\""},
	}

	for i, tt := range tests {
		t.Run(fmt.Sprintf("Test #%d", i+1), func(t *testing.T) {
			err := validateExternalCompressionEngineName(tt.engineName)
			if tt.errStr == "" {
				if err != nil {
					t.Errorf("Expected result \"%v\", got \"%v\"", "<nil>", err)
				}
			} else {
				if !strings.Contains(fmt.Sprintf("%v", err), tt.errStr) {
					t.Errorf("Expected result \"%v\", got \"%v\"", tt.errStr, err)
				}
			}
		})
	}
}
