/*
Copyright 2017 Google Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreedto in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package zk2topo

import (
	"bytes"
	"fmt"
	"path"

	"github.com/samuel/go-zookeeper/zk"
	"golang.org/x/net/context"

	"vitess.io/vitess/go/vt/topo"
)

// Create is part of the topo.Conn interface.
func (zs *Server) Create(ctx context.Context, filePath string, contents []byte) (topo.Version, error) {
	zkPath := path.Join(zs.root, filePath)

	pathCreated, err := CreateRecursive(ctx, zs.conn, zkPath, contents, 0, zk.WorldACL(PermFile), -1)
	if err != nil {
		return nil, convertError(err, zkPath)
	}

	// Now do a Get to get the version. If the content doesn't
	// match, it means someone else already changed the file,
	// between our Create and Get. It is safer to return an error here,
	// and let the calling process recover if it can.
	data, stat, err := zs.conn.Get(ctx, pathCreated)
	if err != nil {
		return nil, convertError(err, zkPath)
	}
	if !bytes.Equal(data, contents) {
		return nil, fmt.Errorf("file contents changed between zk.Create and zk.Get")
	}

	return ZKVersion(stat.Version), nil
}

// Update is part of the topo.Conn interface.
func (zs *Server) Update(ctx context.Context, filePath string, contents []byte, version topo.Version) (topo.Version, error) {
	zkPath := path.Join(zs.root, filePath)

	// Interpret the version
	var zkVersion int32
	if version != nil {
		zkVersion = int32(version.(ZKVersion))
	} else {
		zkVersion = -1
	}

	stat, err := zs.conn.Set(ctx, zkPath, contents, zkVersion)
	if zkVersion == -1 && err == zk.ErrNoNode {
		// In zookeeper, an unconditional set of a nonexisting
		// node will return ErrNoNode. In that case, we want
		// to Create.
		return zs.Create(ctx, filePath, contents)
	}
	if err != nil {
		return nil, convertError(err, zkPath)
	}
	return ZKVersion(stat.Version), nil
}

// Get is part of the topo.Conn interface.
func (zs *Server) Get(ctx context.Context, filePath string) ([]byte, topo.Version, error) {
	zkPath := path.Join(zs.root, filePath)

	contents, stat, err := zs.conn.Get(ctx, zkPath)
	if err != nil {
		return nil, nil, convertError(err, zkPath)
	}
	return contents, ZKVersion(stat.Version), nil
}

// Delete is part of the topo.Conn interface.
func (zs *Server) Delete(ctx context.Context, filePath string, version topo.Version) error {
	zkPath := path.Join(zs.root, filePath)

	// Interpret the version
	var zkVersion int32
	if version != nil {
		zkVersion = int32(version.(ZKVersion))
	} else {
		zkVersion = -1
	}

	if err := zs.conn.Delete(ctx, zkPath, zkVersion); err != nil {
		return convertError(err, zkPath)
	}
	return zs.recursiveDeleteParentIfEmpty(ctx, filePath)
}

func (zs *Server) recursiveDeleteParentIfEmpty(ctx context.Context, filePath string) error {
	dir := path.Dir(filePath)
	if dir == "" || dir == "/" || dir == "." {
		// we reached the top
		return nil
	}
	zkPath := path.Join(zs.root, dir)
	err := zs.conn.Delete(ctx, zkPath, -1)
	switch err {
	case nil:
		// we keep going up
		return zs.recursiveDeleteParentIfEmpty(ctx, dir)
	case zk.ErrNotEmpty, zk.ErrNoNode:
		// we're done (not empty, or someone beat us to deletion)
		return nil
	default:
		return err
	}
}
