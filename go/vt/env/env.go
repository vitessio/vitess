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
// This is the root for the 'vt' distribution, which contains bin/vtaction
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
