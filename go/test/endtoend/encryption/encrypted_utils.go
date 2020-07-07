/*
Copyright 2019 The Vitess Authors.

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

/* This test makes sure encrypted transport over gRPC works.*/

package encryption

import (
	"os"
	"os/exec"
)

// CreateDirectory will create directory with dirName
func CreateDirectory(dirName string, mode os.FileMode) error {
	if _, err := os.Stat(dirName); os.IsNotExist(err) {
		return os.Mkdir(dirName, mode)
	}
	return nil
}

// ExecuteVttlstestCommand executes vttlstest binary with passed args
func ExecuteVttlstestCommand(args ...string) error {
	tmpProcess := exec.Command(
		"vttlstest",
		args...,
	)
	return tmpProcess.Run()
}
