package goimports

import (
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

	cmd := exec.Command("goimports", "-local", "vitess.io/vitess", "-w", tempFile.Name())
	cmd.Stderr = os.Stderr
	err = cmd.Run()
	if err != nil {
		return nil, err
	}
	return os.ReadFile(tempFile.Name())
}
