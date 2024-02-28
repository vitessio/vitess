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

	"vitess.io/vitess/go/vt/log"
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
