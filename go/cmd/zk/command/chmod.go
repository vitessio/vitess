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

	"vitess.io/vitess/go/cmd/zk/internal/zkfilepath"
	"vitess.io/vitess/go/cmd/zk/internal/zkfs"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/topo/zk2topo"
)

var Chmod = &cobra.Command{
	Use: "chmod <mode> <path>",
	Example: `zk chmod n-mode /zk/path
zk chmod n+mode /zk/path`,
	Args: cobra.MinimumNArgs(2),
	RunE: commandChmod,
}

func commandChmod(cmd *cobra.Command, args []string) error {
	mode := cmd.Flags().Arg(0)
	if mode[0] != 'n' {
		return fmt.Errorf("chmod: invalid mode")
	}

	addPerms := false
	if mode[1] == '+' {
		addPerms = true
	} else if mode[1] != '-' {
		return fmt.Errorf("chmod: invalid mode")
	}

	permMask := zkfs.ParsePermMode(mode[2:])

	resolved, err := zk2topo.ResolveWildcards(cmd.Context(), fs.Conn, cmd.Flags().Args()[1:])
	if err != nil {
		return fmt.Errorf("chmod: invalid wildcards: %w", err)
	}
	if len(resolved) == 0 {
		// the wildcards didn't result in anything, we're done
		return nil
	}

	hasError := false
	for _, arg := range resolved {
		zkPath := zkfilepath.Clean(arg)
		aclv, _, err := fs.Conn.GetACL(cmd.Context(), zkPath)
		if err != nil {
			hasError = true
			log.Warningf("chmod: cannot set access %v: %v", zkPath, err)
			continue
		}
		if addPerms {
			aclv[0].Perms |= permMask
		} else {
			aclv[0].Perms &= ^permMask
		}
		err = fs.Conn.SetACL(cmd.Context(), zkPath, aclv, -1)
		if err != nil {
			hasError = true
			log.Warningf("chmod: cannot set access %v: %v", zkPath, err)
			continue
		}
	}
	if hasError {
		return fmt.Errorf("chmod: some paths had errors")
	}
	return nil
}

func init() {
	Root.AddCommand(Chmod)
}
