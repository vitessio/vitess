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

package codegen

import (
	"bytes"
	"fmt"
	"go/format"
	"os"
)

type GoFile struct {
	name string
	buf  bytes.Buffer
}

func (g *GoFile) Write(p []byte) (n int, err error) {
	return g.buf.Write(p)
}

func (g *GoFile) Close() error {
	formatted, err := format.Source(g.buf.Bytes())
	if err != nil {
		fmt.Fprintf(os.Stderr, "bad source:\n%s\n", g.buf.Bytes())
		return fmt.Errorf("failed to format generated code: %v", err)
	}

	err = os.WriteFile(g.name, formatted, 0644)
	if err != nil {
		return fmt.Errorf("failed to generate %q: %v", g.name, err)
	}
	return nil
}

func NewGoFile(path string) *GoFile {
	return &GoFile{name: path}
}
