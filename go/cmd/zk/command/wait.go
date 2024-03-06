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
	"fmt"

	"github.com/spf13/cobra"
	"github.com/z-division/go-zookeeper/zk"

	"vitess.io/vitess/go/cmd/zk/internal/zkfilepath"
)

var (
	waitArgs = struct {
		ExitIfExists bool
	}{}

	Wait = &cobra.Command{
		Use:   "wait <path>",
		Short: "Sets a watch on the node and then waits for an event to fire.",
		Example: ` # Wait for node change or creation.
zk wait /zk/path

# Trailing slash waits on children.
zk wait /zk/path/children/`,
		Args: cobra.ExactArgs(1),
		RunE: commandWait,
	}
)

func commandWait(cmd *cobra.Command, args []string) error {
	zkPath := cmd.Flags().Arg(0)
	isDir := zkPath[len(zkPath)-1] == '/'
	zkPath = zkfilepath.Clean(zkPath)

	var wait <-chan zk.Event
	var err error
	if isDir {
		_, _, wait, err = fs.Conn.ChildrenW(cmd.Context(), zkPath)
	} else {
		_, _, wait, err = fs.Conn.GetW(cmd.Context(), zkPath)
	}
	if err != nil {
		if err == zk.ErrNoNode {
			_, _, wait, _ = fs.Conn.ExistsW(cmd.Context(), zkPath)
		} else {
			return fmt.Errorf("wait: error %v: %v", zkPath, err)
		}
	} else {
		if waitArgs.ExitIfExists {
			return fmt.Errorf("already exists: %v", zkPath)
		}
	}
	event := <-wait
	fmt.Printf("event: %v\n", event)
	return nil
}

func init() {
	Wait.Flags().BoolVarP(&waitArgs.ExitIfExists, "exit", "e", false, "exit if the path already exists")

	Root.AddCommand(Wait)
}
