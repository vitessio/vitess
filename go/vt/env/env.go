/*
Copyright 2017 Google Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreedto in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package env

import (
	"errors"
	"os"
	"path"
	"path/filepath"
	"strings"
)

const (
	DefaultVtDataRoot = "/vt"
)

// VtRoot returns $VTROOT or tries to guess its value if it's not set.
// This is the root for the 'vt' distribution, which contains bin/vttablet
// for instance.
func VtRoot() (root string, err error) {
	if root = os.Getenv("VTROOT"); root != "" {
		return root, nil
	}
	command, err := filepath.Abs(os.Args[0])
	if err != nil {
		return
	}
	dir := path.Dir(command)

	if strings.HasSuffix(dir, "/bin") {
		return path.Dir(dir), nil
	}
	err = errors.New("VTROOT could not be guessed from the executable location. Please set $VTROOT.")
	return
}

// VtDataRoot returns $VTDATAROOT or the default if $VTDATAROOT is not
// set. VtDataRoot does not check if the directory exists and is
// writable.
func VtDataRoot() string {
	if dataRoot := os.Getenv("VTDATAROOT"); dataRoot != "" {
		return dataRoot
	}

	return DefaultVtDataRoot
}

// VtMysqlRoot returns the root for the mysql distribution, which
// contains bin/mysql CLI for instance.
func VtMysqlRoot() (string, error) {
	// if the environment variable is set, use that
	if root := os.Getenv("VT_MYSQL_ROOT"); root != "" {
		return root, nil
	}

	// otherwise let's use VTROOT
	root, err := VtRoot()
	if err != nil {
		return "", errors.New("VT_MYSQL_ROOT is not set and could not be guessed from the executable location. Please set $VT_MYSQL_ROOT.")
	}
	return root, nil
}

// VtMysqlBaseDir returns the Mysql base directory, which
// contains the fill_help_tables.sql script for instance
func VtMysqlBaseDir() (string, error) {
	// if the environment variable is set, use that
	if root := os.Getenv("VT_MYSQL_BASEDIR"); root != "" {
		return root, nil
	}

	// otherwise let's use VtMysqlRoot
	root, err := VtMysqlRoot()
	if err != nil {
		return "", errors.New("VT_MYSQL_BASEDIR is not set. Please set $VT_MYSQL_BASEDIR.")
	}
	return root, nil
}
