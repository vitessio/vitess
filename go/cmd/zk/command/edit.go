/*
Copyright 2023 The Vitess Authors.

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

package command

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"path"
	"time"

	"github.com/spf13/cobra"
	"github.com/z-division/go-zookeeper/zk"

	"vitess.io/vitess/go/cmd/zk/internal/zkfilepath"
	"vitess.io/vitess/go/vt/log"
)

var (
	editArgs = struct {
		Force bool
	}{}

	Edit = &cobra.Command{
		Use:   "edit <path>",
		Short: "Create a local copy, edit, and write changes back to cell.",
		Args:  cobra.ExactArgs(1),
		RunE:  commandEdit,
	}
)

func commandEdit(cmd *cobra.Command, args []string) error {
	arg := cmd.Flags().Arg(0)
	zkPath := zkfilepath.Clean(arg)
	data, stat, err := fs.Conn.Get(cmd.Context(), zkPath)
	if err != nil {
		if !editArgs.Force || err != zk.ErrNoNode {
			log.Warningf("edit: cannot access %v: %v", zkPath, err)
		}
		return fmt.Errorf("edit: cannot access %v: %v", zkPath, err)
	}

	name := path.Base(zkPath)
	tmpPath := fmt.Sprintf("/tmp/zk-edit-%v-%v", name, time.Now().UnixNano())
	f, err := os.Create(tmpPath)
	if err == nil {
		_, err = f.Write(data)
		f.Close()
	}
	if err != nil {
		return fmt.Errorf("edit: cannot write file %v", err)
	}

	editor := exec.Command(os.Getenv("EDITOR"), tmpPath)
	editor.Stdin = os.Stdin
	editor.Stdout = os.Stdout
	editor.Stderr = os.Stderr
	err = editor.Run()
	if err != nil {
		os.Remove(tmpPath)
		return fmt.Errorf("edit: cannot start $EDITOR: %v", err)
	}

	fileData, err := os.ReadFile(tmpPath)
	if err != nil {
		os.Remove(tmpPath)
		return fmt.Errorf("edit: cannot read file %v", err)
	}

	if !bytes.Equal(fileData, data) {
		// data changed - update if we can
		_, err = fs.Conn.Set(cmd.Context(), zkPath, fileData, stat.Version)
		if err != nil {
			os.Remove(tmpPath)
			return fmt.Errorf("edit: cannot write zk file %v", err)
		}
	}
	os.Remove(tmpPath)
	return nil
}

func init() {
	Edit.Flags().BoolVarP(&editArgs.Force, "force", "f", false, "no warning on nonexistent node")

	Root.AddCommand(Edit)
}
