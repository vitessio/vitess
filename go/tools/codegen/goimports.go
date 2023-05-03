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
	"log"
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

	err = GoImports(tempFile.Name())
	if err != nil {
		return nil, err
	}
	return os.ReadFile(tempFile.Name())
}

func GoImports(fullPath string) error {
	// we need to run both gofmt and goimports because goimports does not support the
	// simplification flag (-s) that our static linter checks require.

	cmd := exec.Command("gofmt", "-s", "-w", fullPath)
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		return err
	}

	cmd = exec.Command("goimports", "-local", "vitess.io/vitess", "-w", fullPath)
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		return err
	}

	return nil
}

func SaveJenFile(fullPath string, file *jen.File) error {
	if err := file.Save(fullPath); err != nil {
		return err
	}
	if err := GoImports(fullPath); err != nil {
		return err
	}
	log.Printf("saved '%s'", fullPath)
	return nil
}
