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
	"strings"

	"github.com/spf13/cobra"
	"github.com/z-division/go-zookeeper/zk"

	"vitess.io/vitess/go/cmd/zk/internal/zkfilepath"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/topo/zk2topo"
)

var (
	rmArgs = struct {
		Force           bool
		RecursiveDelete bool
	}{}

	Rm = &cobra.Command{
		Use: "rm <path>",
		Example: `zk rm /zk/path

# Recursive.
zk rm -R /zk/path

# No error on nonexistent node.
zk rm -f /zk/path`,
		Args: cobra.MinimumNArgs(1),
		RunE: commandRm,
	}
)

func commandRm(cmd *cobra.Command, args []string) error {
	if rmArgs.RecursiveDelete {
		for _, arg := range cmd.Flags().Args() {
			zkPath := zkfilepath.Clean(arg)
			if strings.Count(zkPath, "/") < 2 {
				return fmt.Errorf("rm: overly general path: %v", zkPath)
			}
		}
	}

	resolved, err := zk2topo.ResolveWildcards(cmd.Context(), fs.Conn, cmd.Flags().Args())
	if err != nil {
		return fmt.Errorf("rm: invalid wildcards: %v", err)
	}
	if len(resolved) == 0 {
		// the wildcards didn't result in anything, we're done
		return nil
	}

	hasError := false
	for _, arg := range resolved {
		zkPath := zkfilepath.Clean(arg)
		var err error
		if rmArgs.RecursiveDelete {
			err = zk2topo.DeleteRecursive(cmd.Context(), fs.Conn, zkPath, -1)
		} else {
			err = fs.Conn.Delete(cmd.Context(), zkPath, -1)
		}
		if err != nil && (!rmArgs.Force || err != zk.ErrNoNode) {
			hasError = true
			log.Warningf("rm: cannot delete %v: %v", zkPath, err)
		}
	}
	if hasError {
		// to be consistent with the command line 'rm -f', return
		// 0 if using 'zk rm -f' and the file doesn't exist.
		return fmt.Errorf("rm: some paths had errors")
	}
	return nil
}

func init() {
	Rm.Flags().BoolVarP(&rmArgs.Force, "force", "f", false, "no warning on nonexistent node")
	Rm.Flags().BoolVarP(&rmArgs.RecursiveDelete, "recursivedelete", "r", false, "recursive delete")

	Root.AddCommand(Rm)
}
