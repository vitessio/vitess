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

// Package zkfs provides utilities for working with zookeepr in a filesystem-like manner.
package zkfs

import (
	"context"
	"fmt"
	"io"
	"os"
	"path"
	"strings"
	"syscall"

	"github.com/z-division/go-zookeeper/zk"

	"vitess.io/vitess/go/cmd/zk/internal/zkfilepath"
	"vitess.io/vitess/go/vt/topo/zk2topo"
)

// FS wraps a zk2topo connection to provide FS utility methods.
type FS struct {
	Conn *zk2topo.ZkConn
}

// CopyContext copies the contents of src to dst.
func (fs *FS) CopyContext(ctx context.Context, src, dst string) error {
	dstIsDir := dst[len(dst)-1] == '/'
	src = zkfilepath.Clean(src)
	dst = zkfilepath.Clean(dst)

	if !IsFile(src) && !IsFile(dst) {
		return fmt.Errorf("cp: neither src nor dst is a /zk file")
	}

	data, err := fs.ReadContext(ctx, src)
	if err != nil {
		return fmt.Errorf("cp: cannot read %v: %v", src, err)
	}

	// If we are copying to a local directory - say '.', make the filename
	// the same as the source.
	if !IsFile(dst) {
		fileInfo, err := os.Stat(dst)
		if err != nil {
			if err.(*os.PathError).Err != syscall.ENOENT {
				return fmt.Errorf("cp: cannot stat %v: %v", dst, err)
			}
		} else if fileInfo.IsDir() {
			dst = path.Join(dst, path.Base(src))
		}
	} else if dstIsDir {
		// If we are copying into zk, interpret trailing slash as treating the
		// dst as a directory.
		dst = path.Join(dst, path.Base(src))
	}
	if err := fs.WriteContext(ctx, dst, data); err != nil {
		return fmt.Errorf("cp: cannot write %v: %v", dst, err)
	}
	return nil
}

// MultiCopyContext copies the contents of multiple sources to a single dst directory.
func (fs *FS) MultiCopyContext(ctx context.Context, args []string) error {
	dstPath := args[len(args)-1]
	if dstPath[len(dstPath)-1] != '/' {
		// In multifile context, dstPath must be a directory.
		dstPath += "/"
	}

	for _, srcPath := range args[:len(args)-1] {
		if err := fs.CopyContext(ctx, srcPath, dstPath); err != nil {
			return err
		}
	}
	return nil
}

// ReadContext reads the data stored at path.
func (fs *FS) ReadContext(ctx context.Context, path string) (data []byte, err error) {
	if !IsFile(path) {
		data, _, err = fs.Conn.Get(ctx, path)
		return data, err
	}

	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}

	data, err = io.ReadAll(file)
	return data, err
}

// WriteContext writes the given data to path.
func (fs *FS) WriteContext(ctx context.Context, path string, data []byte) (err error) {
	if IsFile(path) {
		_, err = fs.Conn.Set(ctx, path, data, -1)
		if err == zk.ErrNoNode {
			_, err = zk2topo.CreateRecursive(ctx, fs.Conn, path, data, 0, zk.WorldACL(zk.PermAll), 10)
		}
		return err
	}
	return os.WriteFile(path, []byte(data), 0666)
}

var (
	charPermMap map[string]int32
	permCharMap map[int32]string
)

func init() {
	charPermMap = map[string]int32{
		"r": zk.PermRead,
		"w": zk.PermWrite,
		"d": zk.PermDelete,
		"c": zk.PermCreate,
		"a": zk.PermAdmin,
	}
	permCharMap = make(map[int32]string)
	for c, p := range charPermMap {
		permCharMap[p] = c
	}
}

// FormatACL returns a string representation of a zookeeper ACL permission.
func FormatACL(acl zk.ACL) string {
	s := ""

	for _, perm := range []int32{zk.PermRead, zk.PermWrite, zk.PermDelete, zk.PermCreate, zk.PermAdmin} {
		if acl.Perms&perm != 0 {
			s += permCharMap[perm]
		} else {
			s += "-"
		}
	}
	return s
}

// IsFile returns true if the path is a zk type of file.
func IsFile(path string) bool {
	return strings.HasPrefix(path, "/zk")
}

// ParsePermMode parses the mode string as a perm mask.
func ParsePermMode(mode string) (mask int32) {
	if len(mode) < 2 {
		panic("invalid mode")
	}

	for _, c := range mode[2:] {
		mask |= charPermMap[string(c)]
	}

	return mask
}

// Item represents an item in a zookeeper filesystem.
type Item struct {
	Path string
	Data []byte
	Stat *zk.Stat
	Err  error
}
