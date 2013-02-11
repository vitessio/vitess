// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package zkocc

import (
	"bytes"
	"errors"
	"expvar"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"

	"code.google.com/p/vitess/go/relog"
	"code.google.com/p/vitess/go/stats"
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
	mutex        sync.Mutex
	zcell        map[string]*zkCell
	resolveLocal bool
	localCell    string

	// stats, use sync/atomic to access them
	rpcCalls          int32
	unknownCellErrors int32
}

var (
	ErrPartialRead = errors.New("zkocc: partial read")
	ErrLocalCell   = errors.New("zkocc: cannot resolve local cell")
)

func NewZkReader(resolveLocal bool, preload []string) *ZkReader {
	zkr := &ZkReader{zcell: make(map[string]*zkCell), resolveLocal: resolveLocal}
	if resolveLocal {
		zkr.localCell = zk.GuessLocalCell()
	}

	// register to expvar
	expvar.Publish("ZkReader", stats.StrFunc(func() string { return zkr.statsJSON() }))

	// start some cells
	for _, cellName := range preload {
		_, path, err := zkr.getCell("/zk/" + cellName)
		if err != nil {
			relog.Error("Cell " + cellName + " could not be preloaded: " + err.Error())
		} else {
			relog.Info("Cell " + cellName + " preloaded for: " + path)
		}
	}
	return zkr
}

func (zkr *ZkReader) getCell(path string) (*zkCell, string, error) {
	zkr.mutex.Lock()
	defer zkr.mutex.Unlock()
	cellName, err := zk.ZkCellFromZkPath(path)
	if err != nil {
		return nil, "", err
	}

	// the 'local' cell has to be resolved, and the path fixed
	resolvedPath := path
	if cellName == "local" {
		if zkr.resolveLocal {
			cellName = zkr.localCell
			parts := strings.Split(path, "/")
			parts[2] = cellName
			resolvedPath = strings.Join(parts, "/")
		} else {
			return nil, "", ErrLocalCell
		}
	}

	cell, ok := zkr.zcell[cellName]
	if !ok {
		zkaddr, err := zk.ZkPathToZkAddr(path, false)
		if err != nil {
			return nil, "", err
		}
		cell = newZkCell(cellName, zkaddr)
		zkr.zcell[cellName] = cell
	}
	return cell, resolvedPath, nil
}

func handleError(err *error) {
	if x := recover(); x != nil {
		relog.Error("rpc panic: %v", x)
		terr, ok := x.(error)
		if !ok {
			*err = fmt.Errorf("rpc panic: %v", x)
			return
		}
		*err = terr
	}
}

func (zkr *ZkReader) Get(req *proto.ZkPath, reply *proto.ZkNode) (err error) {
	defer handleError(&err)
	atomic.AddInt32(&zkr.rpcCalls, 1)

	return zkr.get(req, reply)
}

func (zkr *ZkReader) get(req *proto.ZkPath, reply *proto.ZkNode) (err error) {
	cell, path, err := zkr.getCell(req.Path)
	if err != nil {
		relog.Warning("Unknown cell for path %v: %v", req.Path, err)
		atomic.AddInt32(&zkr.unknownCellErrors, 1)
		return err
	}

	// check the cell cache
	if cached, stale := cell.zcache.get(path, reply); cached {
		reply.Cached = true
		reply.Stale = stale
		if stale {
			atomic.AddInt32(&cell.staleReads, 1)
		} else {
			atomic.AddInt32(&cell.cacheReads, 1)
		}
		return nil
	}

	// not in cache, have to query zk
	// first get the connection
	zconn, err := cell.getConnection()
	if err != nil {
		relog.Warning("ZK connection error for path %v: %v", req.Path, err)
		atomic.AddInt32(&cell.otherErrors, 1)
		return err
	}

	reply.Path = path
	var stat *zookeeper.Stat
	var watch <-chan zookeeper.Event
	reply.Data, stat, watch, err = zconn.GetW(reply.Path)
	if err != nil {
		relog.Warning("ZK error for path %v: %v", req.Path, err)
		if zookeeper.IsError(err, zookeeper.ZNONODE) {
			atomic.AddInt32(&cell.nodeNotFoundErrors, 1)
		} else {
			atomic.AddInt32(&cell.otherErrors, 1)
		}
		return err
	}
	atomic.AddInt32(&cell.zkReads, 1)
	reply.Stat.FromZookeeperStat(stat)

	// update cache, set channel
	cell.zcache.updateData(path, reply.Data, &reply.Stat, watch)
	return nil
}

func (zkr *ZkReader) GetV(req *proto.ZkPathV, reply *proto.ZkNodeV) (err error) {
	defer handleError(&err)
	atomic.AddInt32(&zkr.rpcCalls, 1)

	wg := sync.WaitGroup{}
	mu := sync.Mutex{}

	reply.Nodes = make([]*proto.ZkNode, len(req.Paths))
	errors := make([]error, 0, len(req.Paths))
	for i, zkPath := range req.Paths {
		wg.Add(1)
		go func(i int, zkPath string) {
			zp := &proto.ZkPath{zkPath}
			zn := &proto.ZkNode{}
			err := zkr.get(zp, zn)
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
		// this won't transmit the responses we actually got,
		// just return an error for them all. Look at logs to
		// figure out what went wrong.
		return ErrPartialRead
	}
	return nil
}

func (zkr *ZkReader) Children(req *proto.ZkPath, reply *proto.ZkNode) (err error) {
	defer handleError(&err)
	atomic.AddInt32(&zkr.rpcCalls, 1)

	cell, path, err := zkr.getCell(req.Path)
	if err != nil {
		return err
	}

	// check the cell cache
	if cached, stale := cell.zcache.children(path, reply); cached {
		reply.Cached = true
		reply.Stale = stale
		if stale {
			atomic.AddInt32(&cell.staleReads, 1)
		} else {
			atomic.AddInt32(&cell.cacheReads, 1)
		}
		return nil
	}

	// not in cache, have to query zk
	// first get the connection
	zconn, err := cell.getConnection()
	if err != nil {
		relog.Warning("ZK connection error for path %v: %v", req.Path, err)
		atomic.AddInt32(&cell.otherErrors, 1)
		return err
	}

	reply.Path = path
	var stat *zookeeper.Stat
	var watch <-chan zookeeper.Event
	reply.Children, stat, watch, err = zconn.ChildrenW(path)
	if err != nil {
		relog.Warning("ZK error for path %v: %v", req.Path, err)
		if zookeeper.IsError(err, zookeeper.ZNONODE) {
			atomic.AddInt32(&cell.nodeNotFoundErrors, 1)
		} else {
			atomic.AddInt32(&cell.otherErrors, 1)
		}
		return err
	}
	atomic.AddInt32(&cell.zkReads, 1)
	reply.Stat.FromZookeeperStat(stat)

	// update cache
	cell.zcache.updateChildren(path, reply.Children, &reply.Stat, watch)
	return nil
}

func (zkr *ZkReader) statsJSON() string {
	zkr.mutex.Lock()
	defer zkr.mutex.Unlock()

	b := bytes.NewBuffer(make([]byte, 0, 4096))
	fmt.Fprintf(b, "{")
	fmt.Fprintf(b, "\"RpcCalls\": %v,", atomic.LoadInt32(&zkr.rpcCalls))
	fmt.Fprintf(b, "\"UnknownCellErrors\": %v", atomic.LoadInt32(&zkr.unknownCellErrors))
	var zkReads int32
	var cacheReads int32
	var staleReads int32
	var nodeNotFoundErrors int32
	var otherErrors int32
	for name, zcell := range zkr.zcell {
		fmt.Fprintf(b, ", \"%v\": %v", name, zcell.String())
		zkReads += atomic.LoadInt32(&zcell.zkReads)
		cacheReads += atomic.LoadInt32(&zcell.cacheReads)
		staleReads += atomic.LoadInt32(&zcell.staleReads)
		nodeNotFoundErrors += atomic.LoadInt32(&zcell.nodeNotFoundErrors)
		otherErrors += atomic.LoadInt32(&zcell.otherErrors)
	}
	fmt.Fprintf(b, ", \"total\": {")
	fmt.Fprintf(b, "\"CacheReads\": %v,", cacheReads)
	fmt.Fprintf(b, "\"NodeNotFoundErrors\": %v,", nodeNotFoundErrors)
	fmt.Fprintf(b, "\"OtherErrors\": %v,", otherErrors)
	fmt.Fprintf(b, "\"StaleReads\": %v,", staleReads)
	fmt.Fprintf(b, "\"ZkReads\": %v", zkReads)
	fmt.Fprintf(b, "}")

	fmt.Fprintf(b, "}")
	return b.String()
}
