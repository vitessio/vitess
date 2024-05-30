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
	"path"
	"sort"
	"sync"

	"github.com/spf13/cobra"
	"github.com/z-division/go-zookeeper/zk"

	"vitess.io/vitess/go/cmd/zk/internal/zkfilepath"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/topo/zk2topo"
)

var (
	lsArgs = struct {
		LongListing      bool
		DirectoryListing bool
		Force            bool
		RecursiveListing bool
	}{}

	Ls = &cobra.Command{
		Use: "ls <path>",
		Example: `zk ls /zk
zk ls -l /zk

# List directory node itself)
zk ls -ld /zk

# Recursive (expensive)
zk ls -R /zk`,
		Args: cobra.MinimumNArgs(1),
		RunE: commandLs,
	}
)

func commandLs(cmd *cobra.Command, args []string) error {
	resolved, err := zk2topo.ResolveWildcards(cmd.Context(), fs.Conn, cmd.Flags().Args())
	if err != nil {
		return fmt.Errorf("ls: invalid wildcards: %v", err)
	}
	if len(resolved) == 0 {
		// the wildcards didn't result in anything, we're
		// done.
		return nil
	}

	hasError := false
	needsHeader := len(resolved) > 1 && !lsArgs.DirectoryListing
	for _, arg := range resolved {
		zkPath := zkfilepath.Clean(arg)
		var children []string
		var err error
		isDir := true
		if lsArgs.DirectoryListing {
			children = []string{""}
			isDir = false
		} else if lsArgs.RecursiveListing {
			children, err = zk2topo.ChildrenRecursive(cmd.Context(), fs.Conn, zkPath)
		} else {
			children, _, err = fs.Conn.Children(cmd.Context(), zkPath)
			// Assume this is a file node if it has no children.
			if len(children) == 0 {
				children = []string{""}
				isDir = false
			}
		}
		if err != nil {
			hasError = true
			if !lsArgs.Force || err != zk.ErrNoNode {
				log.Warningf("ls: cannot access %v: %v", zkPath, err)
			}
		}

		// Show the full path when it helps.
		showFullPath := false
		if lsArgs.RecursiveListing {
			showFullPath = true
		} else if lsArgs.LongListing && (lsArgs.DirectoryListing || !isDir) {
			showFullPath = true
		}
		if needsHeader {
			fmt.Printf("%v:\n", zkPath)
		}
		if len(children) > 0 {
			if lsArgs.LongListing && isDir {
				fmt.Printf("total: %v\n", len(children))
			}
			sort.Strings(children)
			stats := make([]*zk.Stat, len(children))
			wg := sync.WaitGroup{}
			f := func(i int) {
				localPath := path.Join(zkPath, children[i])
				_, stat, err := fs.Conn.Exists(cmd.Context(), localPath)
				if err != nil {
					if !lsArgs.Force || err != zk.ErrNoNode {
						log.Warningf("ls: cannot access: %v: %v", localPath, err)
					}
				} else {
					stats[i] = stat
				}
				wg.Done()
			}
			for i := range children {
				wg.Add(1)
				go f(i)
			}
			wg.Wait()

			for i, child := range children {
				localPath := path.Join(zkPath, child)
				if stat := stats[i]; stat != nil {
					fmt.Println(zkfilepath.Format(stat, localPath, showFullPath, lsArgs.LongListing))
				}
			}
		}
		if needsHeader {
			fmt.Println()
		}
	}
	if hasError {
		return fmt.Errorf("ls: some paths had errors")
	}
	return nil
}

func init() {
	Ls.Flags().BoolVarP(&lsArgs.LongListing, "longlisting", "l", false, "long listing")
	Ls.Flags().BoolVarP(&lsArgs.DirectoryListing, "directorylisting", "d", false, "list directory instead of contents")
	Ls.Flags().BoolVarP(&lsArgs.Force, "force", "f", false, "no warning on nonexistent node")
	Ls.Flags().BoolVarP(&lsArgs.RecursiveListing, "recursivelisting", "R", false, "recursive listing")

	Root.AddCommand(Ls)
}
