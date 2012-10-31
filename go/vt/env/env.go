package env

import (
	"errors"
	"os"
	"path"
	"path/filepath"
	"strings"
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
