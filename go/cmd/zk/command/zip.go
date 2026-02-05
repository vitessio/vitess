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
	"archive/zip"
	"fmt"
	"os"
	"path"
	"strings"
	"sync"

	"github.com/spf13/cobra"

	"vitess.io/vitess/go/cmd/zk/internal/zkfilepath"
	"vitess.io/vitess/go/cmd/zk/internal/zkfs"
	"vitess.io/vitess/go/vt/topo/zk2topo"
)

var Zip = &cobra.Command{
	Use:   "zip <path> [<path> ...] <archive>",
	Short: "Store a zk tree in a zip archive.",
	Long: `Store a zk tree in a zip archive.
	
Note this won't be immediately useful to the local filesystem since znodes can have data and children;
that is, even "directories" can contain data.`,
	Args: cobra.MinimumNArgs(2),
	RunE: commandZip,
}

func commandZip(cmd *cobra.Command, args []string) error {
	posargs := cmd.Flags().Args()
	dstPath := posargs[len(posargs)-1]
	paths := posargs[:len(posargs)-1]
	if !strings.HasSuffix(dstPath, ".zip") {
		return fmt.Errorf("zip: need to specify destination .zip path: %v", dstPath)
	}
	zipFile, err := os.Create(dstPath)
	if err != nil {
		return fmt.Errorf("zip: error %v", err)
	}

	wg := sync.WaitGroup{}
	items := make(chan *zkfs.Item, 64)
	for _, arg := range paths {
		zkPath := zkfilepath.Clean(arg)
		children, err := zk2topo.ChildrenRecursive(cmd.Context(), fs.Conn, zkPath)
		if err != nil {
			return fmt.Errorf("zip: error %v", err)
		}
		for _, child := range children {
			toAdd := path.Join(zkPath, child)
			wg.Add(1)
			go func() {
				data, stat, err := fs.Conn.Get(cmd.Context(), toAdd)
				items <- &zkfs.Item{
					Path: toAdd,
					Data: data,
					Stat: stat,
					Err:  err,
				}
				wg.Done()
			}()
		}
	}
	go func() {
		wg.Wait()
		close(items)
	}()

	zipWriter := zip.NewWriter(zipFile)
	for item := range items {
		path, data, stat, err := item.Path, item.Data, item.Stat, item.Err
		if err != nil {
			return fmt.Errorf("zip: get failed: %v", err)
		}
		// Skip ephemerals - not sure why you would archive them.
		if stat.EphemeralOwner > 0 {
			continue
		}
		fi := &zip.FileHeader{Name: path, Method: zip.Deflate}
		fi.Modified = zk2topo.Time(stat.Mtime)
		f, err := zipWriter.CreateHeader(fi)
		if err != nil {
			return fmt.Errorf("zip: create failed: %v", err)
		}
		_, err = f.Write(data)
		if err != nil {
			return fmt.Errorf("zip: create failed: %v", err)
		}
	}
	err = zipWriter.Close()
	if err != nil {
		return fmt.Errorf("zip: close failed: %v", err)
	}
	zipFile.Close()
	return nil
}

func init() {
	Root.AddCommand(Zip)
}
