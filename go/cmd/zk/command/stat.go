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
	"vitess.io/vitess/go/cmd/zk/internal/zkfs"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/topo/zk2topo"
)

var (
	statArgs = struct {
		Force bool
	}{}
	Stat = &cobra.Command{
		Use:  "stat <path>",
		Args: cobra.MinimumNArgs(1),
		RunE: commandStat,
	}
)

func commandStat(cmd *cobra.Command, args []string) error {
	resolved, err := zk2topo.ResolveWildcards(cmd.Context(), fs.Conn, cmd.Flags().Args())
	if err != nil {
		return fmt.Errorf("stat: invalid wildcards: %v", err)
	}
	if len(resolved) == 0 {
		// the wildcards didn't result in anything, we're done
		return nil
	}

	hasError := false
	for _, arg := range resolved {
		zkPath := zkfilepath.Clean(arg)
		acls, stat, err := fs.Conn.GetACL(cmd.Context(), zkPath)
		if stat == nil {
			err = fmt.Errorf("no such node")
		}
		if err != nil {
			hasError = true
			if !statArgs.Force || err != zk.ErrNoNode {
				log.Warningf("stat: cannot access %v: %v", zkPath, err)
			}
			continue
		}
		fmt.Printf("Path: %s\n", zkPath)
		fmt.Printf("Created: %s\n", zk2topo.Time(stat.Ctime).Format(zkfilepath.TimeFmtMicro))
		fmt.Printf("Modified: %s\n", zk2topo.Time(stat.Mtime).Format(zkfilepath.TimeFmtMicro))
		fmt.Printf("Size: %v\n", stat.DataLength)
		fmt.Printf("Children: %v\n", stat.NumChildren)
		fmt.Printf("Version: %v\n", stat.Version)
		fmt.Printf("Ephemeral: %v\n", stat.EphemeralOwner)
		fmt.Printf("ACL:\n")
		for _, acl := range acls {
			fmt.Printf(" %v:%v %v\n", acl.Scheme, acl.ID, zkfs.FormatACL(acl))
		}
	}
	if hasError {
		return fmt.Errorf("stat: some paths had errors")
	}
	return nil
}

func init() {
	Stat.Flags().BoolVarP(&statArgs.Force, "force", "f", false, "no warning on nonexistent node")

	Root.AddCommand(Stat)
}
