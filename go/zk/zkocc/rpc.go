// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package zkocc

import (
	"bytes"
	"errors"
	"fmt"
	"strings"
	"sync"

	log "github.com/golang/glog"
	"github.com/youtube/vitess/go/stats"
	"github.com/youtube/vitess/go/zk"
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

type zkrStats struct {
	zkReads            *stats.Counters
	cacheReads         *stats.Counters
	staleReads         *stats.Counters
	nodeNotFoundErrors *stats.Counters
	otherErrors        *stats.Counters
}

func newZkrStats() *zkrStats {
	zs := &zkrStats{}
	zs.zkReads = stats.NewCounters("ZkReader-ZkReads")
	zs.cacheReads = stats.NewCounters("ZkReader-CacheReads")
	zs.staleReads = stats.NewCounters("ZkReader-StaleReads")
	zs.nodeNotFoundErrors = stats.NewCounters("ZkReader-NodeNotFoundErrors")
	zs.otherErrors = stats.NewCounters("ZkReader-OtherErrors")
	return zs
}

// ZkReader is the main object receiving RPC calls
type ZkReader struct {
	mutex        sync.Mutex
	zcell        map[string]*zkCell
	resolveLocal bool
	localCell    string

	// stats
	rpcCalls          *stats.Int //sync2.AtomicInt32
	unknownCellErrors *stats.Int //sync2.AtomicInt32
	zkrStats          *zkrStats
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
	zkr.rpcCalls = stats.NewInt("ZkReader-RpcCalls")
	zkr.unknownCellErrors = stats.NewInt("ZkReader-UnknownCellErrors")
	zkr.zkrStats = newZkrStats()

	stats.PublishJSONFunc("ZkReader", zkr.statsJSON)

	// start some cells
	for _, cellName := range preload {
		_, path, err := zkr.getCell("/zk/" + cellName)
		if err != nil {
			log.Errorf("Cell " + cellName + " could not be preloaded: " + err.Error())
		} else {
			log.Infof("Cell " + cellName + " preloaded for: " + path)
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
		cell = newZkCell(cellName, zkaddr, zkr.zkrStats)
		zkr.zcell[cellName] = cell
	}
	return cell, resolvedPath, nil
}

func handleError(err *error) {
	if x := recover(); x != nil {
		log.Errorf("rpc panic: %v", x)
		terr, ok := x.(error)
		if !ok {
			*err = fmt.Errorf("rpc panic: %v", x)
			return
		}
		*err = terr
	}
}

func (zkr *ZkReader) get(req *zk.ZkPath, reply *zk.ZkNode) (err error) {
	// get the cell
	cell, path, err := zkr.getCell(req.Path)
	if err != nil {
		log.Warningf("Unknown cell for path %v: %v", req.Path, err)
		zkr.unknownCellErrors.Add(1)
		return err
	}

	// get the entry
	entry := cell.zcache.getEntry(path)

	// and fill it in if we can
	return entry.get(cell, path, reply)
}

func (zkr *ZkReader) Get(req *zk.ZkPath, reply *zk.ZkNode) (err error) {
	defer handleError(&err)
	zkr.rpcCalls.Add(1)

	return zkr.get(req, reply)
}

func (zkr *ZkReader) GetV(req *zk.ZkPathV, reply *zk.ZkNodeV) (err error) {
	defer handleError(&err)
	zkr.rpcCalls.Add(1)

	wg := sync.WaitGroup{}
	mu := sync.Mutex{}

	reply.Nodes = make([]*zk.ZkNode, len(req.Paths))
	errors := make([]error, 0, len(req.Paths))
	for i, zkPath := range req.Paths {
		wg.Add(1)
		go func(i int, zkPath string) {
			zp := &zk.ZkPath{zkPath}
			zn := &zk.ZkNode{}
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

func (zkr *ZkReader) Children(req *zk.ZkPath, reply *zk.ZkNode) (err error) {
	defer handleError(&err)
	zkr.rpcCalls.Add(1)

	// get the cell
	cell, path, err := zkr.getCell(req.Path)
	if err != nil {
		log.Warningf("Unknown cell for path %v: %v", req.Path, err)
		zkr.unknownCellErrors.Add(1)
		return err
	}

	// get the entry
	entry := cell.zcache.getEntry(path)

	// and fill it in if we can
	return entry.children(cell, path, reply)
}

func (zkr *ZkReader) statsJSON() string {
	zkr.mutex.Lock()
	defer zkr.mutex.Unlock()

	b := bytes.NewBuffer(make([]byte, 0, 4096))
	fmt.Fprintf(b, "{")
	fmt.Fprintf(b, "\"RpcCalls\": %v,", zkr.rpcCalls.Get())
	fmt.Fprintf(b, "\"UnknownCellErrors\": %v", zkr.unknownCellErrors.Get())
	var zkReads int64
	var cacheReads int64
	var staleReads int64
	var nodeNotFoundErrors int64
	var otherErrors int64
	mapZkReads := zkr.zkrStats.zkReads.Counts()
	mapCacheReads := zkr.zkrStats.cacheReads.Counts()
	mapStaleReads := zkr.zkrStats.staleReads.Counts()
	mapNodeNotFoundErrors := zkr.zkrStats.nodeNotFoundErrors.Counts()
	mapOtherErrors := zkr.zkrStats.otherErrors.Counts()
	for name, zcell := range zkr.zcell {
		fmt.Fprintf(b, ", \"%v\": {", name)
		fmt.Fprintf(b, "\"CacheReads\": %v,", mapCacheReads[name])
		fmt.Fprintf(b, "\"NodeNotFoundErrors\": %v,", mapNodeNotFoundErrors[name])
		fmt.Fprintf(b, "\"OtherErrors\": %v,", mapOtherErrors[name])
		fmt.Fprintf(b, "\"StaleReads\": %v,", mapStaleReads[name])
		fmt.Fprintf(b, "\"State\": %v,", zcell.states.String())
		fmt.Fprintf(b, "\"ZkReads\": %v", mapZkReads[name])
		fmt.Fprintf(b, "}")
		zkReads += mapZkReads[name]
		cacheReads += mapCacheReads[name]
		staleReads += mapStaleReads[name]
		nodeNotFoundErrors += mapNodeNotFoundErrors[name]
		otherErrors += mapOtherErrors[name]
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
