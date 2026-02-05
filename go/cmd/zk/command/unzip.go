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
	"io"
	"path"
	"strings"

	"github.com/spf13/cobra"
	"github.com/z-division/go-zookeeper/zk"

	"vitess.io/vitess/go/vt/topo/zk2topo"
)

var Unzip = &cobra.Command{
	Use: "unzip <archive> <path>",
	Example: `zk unzip zktree.zip /
zk unzip zktree.zip /zk/prefix`,
	Args: cobra.ExactArgs(1),
	RunE: commandUnzip,
}

func commandUnzip(cmd *cobra.Command, args []string) error {
	srcPath, dstPath := cmd.Flags().Arg(0), cmd.Flags().Arg(1)

	if !strings.HasSuffix(srcPath, ".zip") {
		return fmt.Errorf("zip: need to specify src .zip path: %v", srcPath)
	}

	zipReader, err := zip.OpenReader(srcPath)
	if err != nil {
		return fmt.Errorf("zip: error %v", err)
	}
	defer zipReader.Close()

	for _, zf := range zipReader.File {
		rc, err := zf.Open()
		if err != nil {
			return fmt.Errorf("unzip: error %v", err)
		}
		data, err := io.ReadAll(rc)
		if err != nil {
			return fmt.Errorf("unzip: failed reading archive: %v", err)
		}
		zkPath := zf.Name
		if dstPath != "/" {
			zkPath = path.Join(dstPath, zkPath)
		}
		_, err = zk2topo.CreateRecursive(cmd.Context(), fs.Conn, zkPath, data, 0, zk.WorldACL(zk.PermAll), 10)
		if err != nil && err != zk.ErrNodeExists {
			return fmt.Errorf("unzip: zk create failed: %v", err)
		}
		_, err = fs.Conn.Set(cmd.Context(), zkPath, data, -1)
		if err != nil {
			return fmt.Errorf("unzip: zk set failed: %v", err)
		}
		rc.Close()
	}
	return nil
}

func init() {
	Root.AddCommand(Unzip)
}
