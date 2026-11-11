/*
Copyright 2026 The Vitess Authors.

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

package zk2topo

import (
	"context"
	"log/slog"
	"path"
	"time"

	"github.com/z-division/go-zookeeper/zk"

	"vitess.io/vitess/go/vt/log"
)

// This file binds znodes written before topo-zk-auth-file was set, or by
// older versions, to the auth-file identity.

// openPerms returns the permissions aclv grants without credentials, through
// world or ip entries.
func openPerms(aclv []zk.ACL) int32 {
	var perms int32
	for _, acl := range aclv {
		if acl.Scheme == "world" || acl.Scheme == "ip" {
			perms |= acl.Perms
		}
	}
	return perms
}

// bindNode replaces the world and ip entries of zkPath's ACL with the same
// permissions for the identities of our session.
func bindNode(ctx context.Context, conn *ZkConn, zkPath string) error {
	var err error
	for range maxAttempts {
		var aclv []zk.ACL
		var stat *zk.Stat
		aclv, stat, err = conn.GetACL(ctx, zkPath)
		if err != nil {
			return err
		}
		perms := openPerms(aclv)
		if perms == 0 {
			return nil
		}
		bound := ACL(perms)
		for _, acl := range aclv {
			if acl.Scheme != "world" && acl.Scheme != "ip" {
				bound = append(bound, acl)
			}
		}
		// The ACL version makes this fail if the ACL changed after we read it.
		err = conn.SetACL(ctx, zkPath, bound, stat.Aversion)
		if err != zk.ErrBadVersion {
			return err
		}
	}
	return err
}

// bindACL binds zkPath and its parents up to zs.root, parents first, so a
// child cannot be deleted and recreated by someone else while it is being
// bound. It returns zk.ErrNoNode if one of them does not exist.
func (zs *Server) bindACL(ctx context.Context, zkPath string) error {
	if authFile == "" {
		return nil
	}
	var nodes []string
	for p := zkPath; len(p) >= len(zs.root) && p != "/"; p = path.Dir(p) {
		nodes = append(nodes, p)
	}
	for i := len(nodes) - 1; i >= 0; i-- {
		if err := bindNode(ctx, zs.conn, nodes[i]); err != nil {
			return err
		}
	}
	return nil
}

// bindChildren binds every znode below dir. A directory is bound before it
// is listed, so nobody else can add children to it that we do not see.
func bindChildren(ctx context.Context, conn *ZkConn, dir string) error {
	children, _, err := conn.Children(ctx, dir)
	if err == zk.ErrNoNode || err == zk.ErrNoAuth {
		return nil
	}
	if err != nil {
		return err
	}
	for _, child := range children {
		p := path.Join(dir, child)
		if err := bindNode(ctx, conn, p); err != nil && err != zk.ErrNoNode && err != zk.ErrNoAuth {
			return err
		}
		if err := bindChildren(ctx, conn, p); err != nil {
			return err
		}
	}
	return nil
}

// bindTree binds every znode under root. root is bound last, so a bound root
// tells later callers the tree is done. Until root is bound anyone can add
// children to it, so the tree is walked once more after that.
func bindTree(ctx context.Context, conn *ZkConn, root string) error {
	aclv, _, err := conn.GetACL(ctx, root)
	if err == zk.ErrNoNode {
		return nil
	}
	if err != nil {
		return err
	}
	if openPerms(aclv) == 0 {
		return nil
	}
	if err := bindChildren(ctx, conn, root); err != nil {
		return err
	}
	if err := bindNode(ctx, conn, root); err != nil {
		return err
	}
	return bindChildren(ctx, conn, root)
}

// migrateACLs runs bindTree on zs.root until it succeeds or ctx is done.
func migrateACLs(ctx context.Context, conn *ZkConn, root string) {
	for {
		err := bindTree(ctx, conn, root)
		if err == nil || ctx.Err() != nil {
			return
		}
		log.Warn("Failed to bind existing znode ACLs to topo-zk-auth-file, will retry", slog.String("root", root), slog.Any("error", err))
		select {
		case <-ctx.Done():
			return
		case <-time.After(baseTimeout):
		}
	}
}
