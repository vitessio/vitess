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

package fileutil

import (
	"errors"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"strings"
)

var ErrInvalidBackupDir = errors.New("invalid backup directory")

// SafePathJoin joins file paths using a rootPath and one or many other paths,
// returning a single absolute path. An error is returned if the joined path
// causes a directory traversal to a path outside of the provided rootPath.
func SafePathJoin(rootPath string, joinPaths ...string) (string, error) {
	allPaths := []string{rootPath}
	allPaths = append(allPaths, joinPaths...)
	p := path.Join(allPaths...)
	absPath, err := filepath.Abs(p)
	if err != nil {
		return p, fmt.Errorf("failed to parse backup path %q: %w", p, err)
	}
	absRootPath, err := filepath.Abs(rootPath)
	if err != nil {
		return p, fmt.Errorf("failed to parse backup root path %q: %w", rootPath, err)
	}
	if absPath != absRootPath && !strings.HasPrefix(absPath, absRootPath+string(os.PathSeparator)) {
		return p, ErrInvalidBackupDir
	}
	return absPath, nil
}
