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
