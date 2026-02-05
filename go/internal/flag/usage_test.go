/*
Copyright 2024 The Vitess Authors.

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
	"io"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSetUsage(t *testing.T) {
	fs := goflag.NewFlagSet("test", goflag.ExitOnError)
	fs.String("testflag", "default", "`test` flag")

	opts := UsageOptions{
		Preface: func(w io.Writer) {
			_, _ = w.Write([]byte("test preface"))
		},
		Epilogue: func(w io.Writer) {
			_, _ = w.Write([]byte("test epilogue"))
		},
		FlagFilter: func(f *goflag.Flag) bool {
			return f.Value.String() == "default"
		},
	}

	SetUsage(fs, opts)

	var builder strings.Builder
	fs.SetOutput(&builder)

	_ = fs.Set("testflag", "not default")
	fs.Usage()

	output := builder.String()
	assert.NotContains(t, output, "test flag")

	// Set the value back to default
	_ = fs.Set("testflag", "default")
	fs.Usage()
	output = builder.String()

	assert.Contains(t, output, "test preface")
	assert.Contains(t, output, "--testflag test")
	assert.Contains(t, output, "test epilogue")
	assert.Contains(t, output, "test flag")
}

func TestSetUsageWithNilFlagFilterAndPreface(t *testing.T) {
	oldOsArgs := os.Args
	defer func() {
		os.Args = oldOsArgs
	}()

	os.Args = []string{"testOsArg"}
	fs := goflag.NewFlagSet("test", goflag.ExitOnError)
	fs.String("testflag", "default", "`test` flag")

	opts := UsageOptions{
		Epilogue: func(w io.Writer) {
			_, _ = w.Write([]byte("test epilogue"))
		},
	}

	SetUsage(fs, opts)

	var builder strings.Builder
	fs.SetOutput(&builder)
	fs.Usage()
	output := builder.String()

	assert.Contains(t, output, "Usage of testOsArg:")
	assert.Contains(t, output, "--testflag test")
	assert.Contains(t, output, "test epilogue")
}

func TestSetUsageWithBoolFlag(t *testing.T) {
	fs := goflag.NewFlagSet("test2", goflag.ExitOnError)
	var tBool bool
	fs.BoolVar(&tBool, "t", true, "`t` flag")

	opts := UsageOptions{
		Preface: func(w io.Writer) {
			_, _ = w.Write([]byte("test preface"))
		},
		Epilogue: func(w io.Writer) {
			_, _ = w.Write([]byte("test epilogue"))
		},
		FlagFilter: func(f *goflag.Flag) bool {
			return f.Value.String() == "true"
		},
	}

	SetUsage(fs, opts)

	var builder strings.Builder
	fs.SetOutput(&builder)
	fs.Usage()
	output := builder.String()

	assert.Contains(t, output, "test preface")
	assert.Contains(t, output, "-t\tt flag")
}
