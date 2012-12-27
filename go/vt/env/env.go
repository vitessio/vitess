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
