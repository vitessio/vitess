package onlineddl

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"vitess.io/vitess/go/vt/log"
)

const (
	readableTimeFormat = "20060102150405"
)

// execCmd searches the PATH for a command and runs it, logging the output.
// If input is not nil, pipe it to the command's stdin.
func execCmd(name string, args, env []string, dir string, input io.Reader, output io.Writer) (cmd *exec.Cmd, err error) {
	cmdPath, err := exec.LookPath(name)
	if err != nil {
		return cmd, err
	}
	log.Infof("execCmd: %v %v %v", name, cmdPath, args)

	cmd = exec.Command(cmdPath, args...)
	cmd.Env = env
	cmd.Dir = dir
	if input != nil {
		cmd.Stdin = input
	}
	if output != nil {
		cmd.Stdout = output
		cmd.Stderr = output
	}
	err = cmd.Run()
	if err != nil {
		err = fmt.Errorf("failed running command: %v %s; error=%v", name, strings.Join(args, " "), err)
		log.Errorf(err.Error())
	}
	log.Infof("execCmd success: %v", name)
	return cmd, err
}

// createTempDir creates a temporary directory and returns its name
func createTempDir(hint string) (dirName string, err error) {
	if hint != "" {
		return os.MkdirTemp("", fmt.Sprintf("online-ddl-%s-*", hint))
	}
	return os.MkdirTemp("", "online-ddl-*")
}

// createTempScript creates an executable file in given directory and with given text as content.
func createTempScript(dirName, fileName, text string) (fullName string, err error) {
	fullName = filepath.Join(dirName, fileName)
	bytes := []byte(text)
	err = os.WriteFile(fullName, bytes, 0755)
	return fullName, err
}

// RandomHash returns a 64 hex character random string
func RandomHash() string {
	size := 64
	rb := make([]byte, size)
	_, _ = rand.Read(rb)

	hasher := sha256.New()
	hasher.Write(rb)
	return hex.EncodeToString(hasher.Sum(nil))
}

// ToReadableTimestamp returns a timestamp, in seconds resolution, that is human readable
// (as opposed to unix timestamp which is just a number)
// Example: for Aug 25 2020, 16:04:25 we return "20200825160425"
func ToReadableTimestamp(t time.Time) string {
	return t.Format(readableTimeFormat)
}

// ReadableTimestamp returns a timestamp, in seconds resolution, that is human readable
// (as opposed to unix timestamp which is just a number), and which corresponds to the time now.
// Example: for Aug 25 2020, 16:04:25 we return "20200825160425"
func ReadableTimestamp() string {
	return ToReadableTimestamp(time.Now())
}
