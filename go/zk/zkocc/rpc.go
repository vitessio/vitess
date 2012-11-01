// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package zkocc

import (
	"errors"
	"strings"
	"sync"

	"code.google.com/p/vitess/go/relog"
	"code.google.com/p/vitess/go/zk"
	"code.google.com/p/vitess/go/zk/zkocc/proto"
	"launchpad.net/gozk/zookeeper"
)

// zkocc
//
// Cache open zk connections and allow cheap read requests.
// Cache data coming back from zk.
//  * Use node notifications for data invalidation and freshness.
//  * Force invalidation after some amount of time with some amount of skew.
//  * Insulate this process from periodic zk cell failures.
//    * On reconnect, re-read all paths.
//    * Report ZkNode as stale when things are disconnected?

// ZkReader is the main object receiving RPC calls
type ZkReader struct {
	mutex     sync.Mutex
	zcell     map[string]*zkCell
	localCell string
}

var (
	ErrPartialRead = errors.New("zkocc: partial read")
)

func NewZkReader(preload []string) *ZkReader {
	zkr := &ZkReader{zcell: make(map[string]*zkCell), localCell: zk.GuessLocalCell()}

	// start some cells
	for _, cellName := range preload {
		_, path := zkr.getCell("/zk/" + cellName)
		relog.Info("Cell " + cellName + " preloaded for: " + path)
	}
	return zkr
}

func (zkr *ZkReader) getCell(path string) (*zkCell, string) {
	zkr.mutex.Lock()
	cellName := zk.ZkCellFromZkPath(path)

	// the 'local' cell has to be resolved, and the path fixed
	resolvedPath := path
	if cellName == "local" {
		cellName = zkr.localCell
		parts := strings.Split(path, "/")
		parts[2] = cellName
		resolvedPath = strings.Join(parts, "/")
	}

	cell, ok := zkr.zcell[cellName]
	if !ok {
		zkaddr := zk.ZkPathToZkAddr(path, false)
		cell = newZkCell(cellName, zkaddr)
		zkr.zcell[cellName] = cell
	}
	zkr.mutex.Unlock()
	return cell, resolvedPath
}

func (zkr *ZkReader) Get(req *proto.ZkPath, reply *proto.ZkNode) (err error) {
	cell, path := zkr.getCell(req.Path)

	// check the cell cache
	if cached, stale := cell.zcache.get(path, reply); cached {
		reply.Cached = true
		reply.Stale = stale
		return
	}

	// not in cache, have to query zk
	// first get the connection
	zconn, err := cell.getConnection()
	if err != nil {
		return err
	}

	reply.Path = path
	var stat *zookeeper.Stat
	var watch <-chan zookeeper.Event
	reply.Data, stat, watch, err = zconn.GetW(reply.Path)
	if err != nil {
		return err
	}
	reply.Stat.FromZookeeperStat(stat)

	// update cache, set channel
	cell.zcache.updateData(path, reply.Data, &reply.Stat, watch)
	return
}

func (zkr *ZkReader) GetV(req *proto.ZkPathV, reply *proto.ZkNodeV) error {
	wg := sync.WaitGroup{}
	mu := sync.Mutex{}

	reply.Nodes = make([]*proto.ZkNode, len(req.Paths))
	errors := make([]error, 0, len(req.Paths))
	for i, zkPath := range req.Paths {
		wg.Add(1)
		go func(i int, zkPath string) {
			zp := &proto.ZkPath{zkPath}
			zn := &proto.ZkNode{}
			err := zkr.Get(zp, zn)
			if err != nil {
				mu.Lock()
				errors = append(errors, err)
				mu.Unlock()
			} else {
				reply.Nodes[i] = zn
			}
			wg.Done()
		}(i, zkPath)
	}
	wg.Wait()
	mu.Lock()
	defer mu.Unlock()
	if len(errors) > 0 {
		// FIXME(alainjobart) this won't transmit the responses
		// we actually got, just return an error for them all
		return ErrPartialRead
	}
	return nil
}

func (zkr *ZkReader) Children(req *proto.ZkPath, reply *proto.ZkNode) (err error) {
	cell, path := zkr.getCell(req.Path)

	// check the cell cache
	if cached, stale := cell.zcache.children(path, reply); cached {
		reply.Cached = true
		reply.Stale = stale
		return
	}

	// not in cache, have to query zk
	// first get the connection
	zconn, err := cell.getConnection()
	if err != nil {
		return err
	}

	reply.Path = path
	var stat *zookeeper.Stat
	var watch <-chan zookeeper.Event
	reply.Children, stat, watch, err = zconn.ChildrenW(path)
	if err != nil {
		return err
	}
	reply.Stat.FromZookeeperStat(stat)

	// update cache
	cell.zcache.updateChildren(path, reply.Children, &reply.Stat, watch)
	return
}
