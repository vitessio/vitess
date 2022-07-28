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

package goimports

import (
	"os"
	"os/exec"

	"github.com/dave/jennifer/jen"
)

// FormatJenFile formats the given *jen.File with goimports and return a slice
// of byte corresponding to the formatted file.
func FormatJenFile(file *jen.File) ([]byte, error) {
	tempFile, err := os.CreateTemp("/tmp", "*.go")
	if err != nil {
		return nil, err
	}

	err = file.Save(tempFile.Name())
	if err != nil {
		return nil, err
	}

	cmd := exec.Command("goimports", "-local", "vitess.io/vitess", "-w", tempFile.Name())
	cmd.Stderr = os.Stderr
	err = cmd.Run()
	if err != nil {
		return nil, err
	}
	return os.ReadFile(tempFile.Name())
}
