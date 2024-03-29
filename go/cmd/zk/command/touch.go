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
	"vitess.io/vitess/go/vt/topo/zk2topo"
)

var (
	touchArgs = struct {
		CreateParents bool
		TouchOnly     bool
	}{}

	Touch = &cobra.Command{
		Use:   "touch <path>",
		Short: "Change node access time.",
		Long: `Change node access time.
		
NOTE: There is no mkdir - just touch a node.
The disntinction between file and directory is not relevant in zookeeper.`,
		Example: `zk touch /zk/path

# Don't create, just touch timestamp.
zk touch -c /zk/path

# Create all parts necessary (think mkdir -p).
zk touch -p /zk/path`,
		Args: cobra.ExactArgs(1),
		RunE: commandTouch,
	}
)

func commandTouch(cmd *cobra.Command, args []string) error {
	zkPath := zkfilepath.Clean(cmd.Flags().Arg(0))
	var (
		version int32 = -1
		create        = false
	)

	data, stat, err := fs.Conn.Get(cmd.Context(), zkPath)
	switch {
	case err == nil:
		version = stat.Version
	case err == zk.ErrNoNode:
		create = true
	default:
		return fmt.Errorf("touch: cannot access %v: %v", zkPath, err)
	}

	switch {
	case !create:
		_, err = fs.Conn.Set(cmd.Context(), zkPath, data, version)
	case touchArgs.TouchOnly:
		return fmt.Errorf("touch: no such path %v", zkPath)
	case touchArgs.CreateParents:
		_, err = zk2topo.CreateRecursive(cmd.Context(), fs.Conn, zkPath, data, 0, zk.WorldACL(zk.PermAll), 10)
	default:
		_, err = fs.Conn.Create(cmd.Context(), zkPath, data, 0, zk.WorldACL(zk.PermAll))
	}

	if err != nil {
		return fmt.Errorf("touch: cannot modify %v: %v", zkPath, err)
	}
	return nil
}

func init() {
	Touch.Flags().BoolVarP(&touchArgs.CreateParents, "createparent", "p", false, "create parents")
	Touch.Flags().BoolVarP(&touchArgs.TouchOnly, "touchonly", "c", false, "touch only - don't create")

	Root.AddCommand(Touch)
}
