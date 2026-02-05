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
	"os"

	"github.com/spf13/cobra"
	"github.com/z-division/go-zookeeper/zk"
	"golang.org/x/term"

	"vitess.io/vitess/go/cmd/zk/internal/zkfilepath"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/zk2topo"
)

var (
	catArgs = struct {
		LongListing bool
		Force       bool
		DecodeProto bool
	}{}

	Cat = &cobra.Command{
		Use: "cat <path1> [<path2> ...]",
		Example: `zk cat /zk/path

# List filename before file data
zk cat -l /zk/path1 /zk/path2`,
		Args: cobra.MinimumNArgs(1),
		RunE: commandCat,
	}
)

func commandCat(cmd *cobra.Command, args []string) error {
	resolved, err := zk2topo.ResolveWildcards(cmd.Context(), fs.Conn, cmd.Flags().Args())
	if err != nil {
		return fmt.Errorf("cat: invalid wildcards: %w", err)
	}
	if len(resolved) == 0 {
		// the wildcards didn't result in anything, we're done
		return nil
	}

	hasError := false
	for _, arg := range resolved {
		zkPath := zkfilepath.Clean(arg)
		data, _, err := fs.Conn.Get(cmd.Context(), zkPath)
		if err != nil {
			hasError = true
			if !catArgs.Force || err != zk.ErrNoNode {
				log.Warningf("cat: cannot access %v: %v", zkPath, err)
			}
			continue
		}

		if catArgs.LongListing {
			fmt.Printf("%v:\n", zkPath)
		}
		decoded := ""
		if catArgs.DecodeProto {
			decoded, err = topo.DecodeContent(zkPath, data, false)
			if err != nil {
				log.Warningf("cat: cannot proto decode %v: %v", zkPath, err)
				decoded = string(data)
			}
		} else {
			decoded = string(data)
		}
		fmt.Print(decoded)
		if len(decoded) > 0 && decoded[len(decoded)-1] != '\n' && (term.IsTerminal(int(os.Stdout.Fd())) || catArgs.LongListing) {
			fmt.Print("\n")
		}
	}
	if hasError {
		return fmt.Errorf("cat: some paths had errors")
	}
	return nil
}

func init() {
	Cat.Flags().BoolVarP(&catArgs.LongListing, "longListing", "l", false, "long listing")
	Cat.Flags().BoolVarP(&catArgs.Force, "force", "f", false, "no warning on nonexistent node")
	Cat.Flags().BoolVarP(&catArgs.DecodeProto, "decodeProto", "p", false, "decode proto files and display them as text")

	Root.AddCommand(Cat)
}
