/*
Copyright 2025 The Vitess Authors.

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

package os2

import (
	"io/fs"
	"os"
)

const (
	// PermFile is a FileMode for regular files without world permission bits.
	PermFile fs.FileMode = 0o660
	// PermDirectory is a FileMode for directories without world permission bits.
	PermDirectory fs.FileMode = 0o770
)

// Create is identical to os.Create except uses 0660 permission
// rather than 0666, to exclude world read/write bit.
func Create(name string) (*os.File, error) {
	return os.OpenFile(name, os.O_RDWR|os.O_CREATE|os.O_TRUNC, PermFile)
}

// WriteFile is identical to os.WriteFile except permission of 0660 is used.
func WriteFile(name string, data []byte) error {
	return os.WriteFile(name, data, PermFile)
}

// Mkdir is identical to os.Mkdir except permission of 0770 is used.
func Mkdir(path string) error {
	return os.Mkdir(path, PermDirectory)
}

// MkdirAll is identical to os.MkdirAll except permission of 0770 is used.
func MkdirAll(path string) error {
	return os.MkdirAll(path, PermDirectory)
}
