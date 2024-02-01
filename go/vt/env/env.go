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

package env

import (
	"errors"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"strings"
)

const (
	// DefaultVtDataRoot is the default value for VTROOT environment variable
	DefaultVtDataRoot = "/vt"
	// DefaultVtRoot is only required for hooks
	DefaultVtRoot  = "/usr/local/vitess"
	mysqldSbinPath = "/usr/sbin/mysqld"
)

var errMysqldNotFound = errors.New("VT_MYSQL_ROOT is not set and no mysqld could be found in your PATH")

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
	return DefaultVtRoot, nil
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

// VtMysqlRoot returns the root for the mysql distribution,
// which contains the bin/mysql CLI for instance.
// If $VT_MYSQL_ROOT is not set, look for mysqld in the $PATH.
func VtMysqlRoot() (string, error) {
	// If the environment variable is set, use that.
	if root := os.Getenv("VT_MYSQL_ROOT"); root != "" {
		return root, nil
	}

	getRoot := func(path string) string {
		return filepath.Dir(filepath.Dir(path)) // Strip mysqld and [s]bin parts
	}
	binpath, err := exec.LookPath("mysqld")
	if err != nil {
		// First see if /usr/sbin/mysqld exists as it might not be in
		// the PATH by default and this is often the default location
		// used by mysqld OS system packages (apt, dnf, etc).
		fi, err := os.Stat(mysqldSbinPath)
		if err == nil /* file exists */ && fi.Mode().IsRegular() /* not a DIR or other special file */ &&
			fi.Mode()&0111 != 0 /* executable by anyone */ {
			return getRoot(mysqldSbinPath), nil
		}
		return "", errMysqldNotFound
	}
	return getRoot(binpath), nil
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
		return "", errors.New("VT_MYSQL_BASEDIR is not set. Please set $VT_MYSQL_BASEDIR")
	}
	return root, nil
}
