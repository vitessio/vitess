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
	"fmt"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"strings"

	"vitess.io/vitess/go/vt/log"
)

const (
	// DefaultVtDataRoot is the default value for VTROOT environment variable
	DefaultVtDataRoot = "/vt"
	// DefaultVtRoot is only required for hooks
	DefaultVtRoot = "/usr/local/vitess"
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
// which contains bin/mysql CLI for instance.
// If it is not set, look for mysqld in the path.
func VtMysqlRoot() (string, error) {
	// if the environment variable is set, use that
	if root := os.Getenv("VT_MYSQL_ROOT"); root != "" {
		return root, nil
	}

	// otherwise let's look for mysqld in the PATH.
	// ensure that /usr/sbin is included, as it might not be by default
	// This is the default location for mysqld from packages.
	newPath := fmt.Sprintf("/usr/sbin:%s", os.Getenv("PATH"))
	os.Setenv("PATH", newPath)
	path, err := exec.LookPath("mysqld")
	if err != nil {
		return "", errors.New("VT_MYSQL_ROOT is not set and no mysqld could be found in your PATH")
	}
	path = filepath.Dir(filepath.Dir(path)) // strip mysqld, and the sbin
	return path, nil
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

// CheckPlannerVersionFlag takes two string references and checks that just one
// has a value or that they agree with each other.
func CheckPlannerVersionFlag(correct, deprecated *string) (string, error) {
	if deprecated == nil || *deprecated == "" {
		if correct == nil {
			return "", nil
		}
		return *correct, nil
	}
	if correct == nil || *correct == "" {
		// we know deprecated is not nil here
		log.Warningf("planner_version is deprecated. please use planner-version instead")
		return *deprecated, nil
	}
	if *deprecated != *correct {
		return "", fmt.Errorf("can't specify planner-version and planner_version with different versions")
	}
	return *deprecated, nil
}
