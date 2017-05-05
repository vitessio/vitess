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

package zktopo

import (
	"bytes"
	"fmt"
	"path"

	zookeeper "github.com/samuel/go-zookeeper/zk"
	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/zk"
)

// FIXME(alainjobart) Need to intercept these calls for existing objects.
// For now, this is only used for new objects, so it doesn't matter.

// Create is part of the topo.Backend interface.
func (zkts *Server) Create(ctx context.Context, cell, filePath string, contents []byte) (topo.Version, error) {
	zkPath := path.Join(zkPathForCell(cell), filePath)
	pathCreated, err := zk.CreateRecursive(zkts.zconn, zkPath, contents, 0, zookeeper.WorldACL(zk.PermFile))
	if err != nil {
		return nil, convertError(err)
	}

	// Now do a Get to get the version. If the contents doesn't
	// match, it means someone else already changed the file,
	// between our Create and Get. It is safer to return an error here,
	// and let the calling process recover if it can.
	data, stat, err := zkts.zconn.Get(pathCreated)
	if err != nil {
		return nil, convertError(err)
	}
	if bytes.Compare(data, contents) != 0 {
		return nil, fmt.Errorf("file contents changed between zk.Create and zk.Get")
	}

	return ZKVersion(stat.Version), nil
}

// Update is part of the topo.Backend interface.
func (zkts *Server) Update(ctx context.Context, cell, filePath string, contents []byte, version topo.Version) (topo.Version, error) {
	zkPath := path.Join(zkPathForCell(cell), filePath)

	// Interpret the version
	var zkVersion int32
	if version != nil {
		zkVersion = int32(version.(ZKVersion))
	} else {
		zkVersion = -1
	}

	stat, err := zkts.zconn.Set(zkPath, contents, zkVersion)
	if zkVersion == -1 && err == zookeeper.ErrNoNode {
		// In zookeeper, an unconditional set of a nonexisting
		// node will return ErrNoNode. In that case, we want
		// to Create.
		return zkts.Create(ctx, cell, filePath, contents)
	}
	if err != nil {
		return nil, convertError(err)
	}
	return ZKVersion(stat.Version), nil
}

// Get is part of the topo.Backend interface.
func (zkts *Server) Get(ctx context.Context, cell, filePath string) ([]byte, topo.Version, error) {
	zkPath := path.Join(zkPathForCell(cell), filePath)
	contents, stat, err := zkts.zconn.Get(zkPath)
	if err != nil {
		return nil, nil, convertError(err)
	}
	return contents, ZKVersion(stat.Version), nil
}

// Delete is part of the topo.Backend interface.
func (zkts *Server) Delete(ctx context.Context, cell, filePath string, version topo.Version) error {
	zkPath := path.Join(zkPathForCell(cell), filePath)

	// Interpret the version
	var zkVersion int32
	if version != nil {
		zkVersion = int32(version.(ZKVersion))
	} else {
		zkVersion = -1
	}

	if err := zkts.zconn.Delete(zkPath, zkVersion); err != nil {
		return convertError(err)
	}
	return zkts.recursiveDeleteParentIfEmpty(ctx, cell, filePath)
}

func (zkts *Server) recursiveDeleteParentIfEmpty(ctx context.Context, cell, filePath string) error {
	dir := path.Dir(filePath)
	if dir == "" || dir == "/" || dir == "." {
		// we reached the top
		return nil
	}
	zkPath := path.Join(zkPathForCell(cell), dir)
	switch err := zkts.zconn.Delete(zkPath, -1); err {
	case nil:
		// we keep going up
		return zkts.recursiveDeleteParentIfEmpty(ctx, cell, dir)
	case zookeeper.ErrNotEmpty, zookeeper.ErrNoNode:
		// we're done (not empty, or someone beat us to deletion)
		return nil
	default:
		return err
	}
}
