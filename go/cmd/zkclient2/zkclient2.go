// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package main

import (
	"flag"
	"fmt"
	"os"
	"sort"
	"time"

	log "github.com/golang/glog"
	"github.com/youtube/vitess/go/rpcplus"
	"github.com/youtube/vitess/go/rpcwrap/bsonrpc"
	"github.com/youtube/vitess/go/sync2"
	"github.com/youtube/vitess/go/vt/logutil"
	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/zk"
)

var usage = `
Queries the zkocc zookeeper cache, for test purposes. In get mode, if more
than one value is asked for, will use getv.
`
var mode = flag.String("mode", "get", "which operation to run on the node (get, children, qps, qps2)")
var server = flag.String("server", "localhost:3801", "zkocc server to dial")
var timeout = flag.Duration("timeout", 5*time.Second, "connection timeout")

func init() {
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage of %s:\n", os.Args[0])
		flag.PrintDefaults()
		fmt.Fprintf(os.Stderr, usage)
	}
}

func connect() *rpcplus.Client {
	rpcClient, err := bsonrpc.DialHTTP("tcp", *server, *timeout, nil)
	if err != nil {
		log.Fatalf("Can't connect to zkocc: %v", err)
	}
	return rpcClient
}

func get(rpcClient *rpcplus.Client, path string, verbose bool) {
	// it's a get
	zkPath := &zk.ZkPath{Path: path}
	zkNode := &zk.ZkNode{}
	if err := rpcClient.Call("ZkReader.Get", zkPath, zkNode); err != nil {
		log.Fatalf("ZkReader.Get error: %v", err)
	}
	if verbose {
		println(fmt.Sprintf("%v = %v (NumChildren=%v, Version=%v, Cached=%v, Stale=%v)", zkNode.Path, zkNode.Data, zkNode.Stat.NumChildren(), zkNode.Stat.Version(), zkNode.Cached, zkNode.Stale))
	}

}

func getv(rpcClient *rpcplus.Client, paths []string, verbose bool) {
	zkPathV := &zk.ZkPathV{Paths: make([]string, len(paths))}
	for i, v := range paths {
		zkPathV.Paths[i] = v
	}
	zkNodeV := &zk.ZkNodeV{}
	if err := rpcClient.Call("ZkReader.GetV", zkPathV, zkNodeV); err != nil {
		log.Fatalf("ZkReader.GetV error: %v", err)
	}
	if verbose {
		for i, zkNode := range zkNodeV.Nodes {
			println(fmt.Sprintf("[%v] %v = %v (NumChildren=%v, Version=%v, Cached=%v, Stale=%v)", i, zkNode.Path, zkNode.Data, zkNode.Stat.NumChildren(), zkNode.Stat.Version(), zkNode.Cached, zkNode.Stale))
		}
	}
}

func children(rpcClient *rpcplus.Client, paths []string, verbose bool) {
	for _, v := range paths {
		zkPath := &zk.ZkPath{Path: v}
		zkNode := &zk.ZkNode{}
		if err := rpcClient.Call("ZkReader.Children", zkPath, zkNode); err != nil {
			log.Fatalf("ZkReader.Children error: %v", err)
		}
		if verbose {
			println(fmt.Sprintf("Path = %v", zkNode.Path))
			for i, child := range zkNode.Children {
				println(fmt.Sprintf("Child[%v] = %v", i, child))
			}
			println(fmt.Sprintf("NumChildren = %v", zkNode.Stat.NumChildren()))
			println(fmt.Sprintf("CVersion = %v", zkNode.Stat.CVersion()))
			println(fmt.Sprintf("Cached = %v", zkNode.Cached))
			println(fmt.Sprintf("Stale = %v", zkNode.Stale))
		}
	}
}

func getSrvKeyspaceNames(rpcClient *rpcplus.Client, cell string, verbose bool) {
	req := &topo.GetSrvKeyspaceNamesArgs{
		Cell: cell,
	}
	reply := &topo.SrvKeyspaceNames{}
	if err := rpcClient.Call("TopoReader.GetSrvKeyspaceNames", req, reply); err != nil {
		log.Fatalf("TopoReader.GetSrvKeyspaceNames error: %v", err)
	}
	if verbose {
		for i, entry := range reply.Entries {
			println(fmt.Sprintf("KeyspaceNames[%v] = %v", i, entry))
		}
	}
}

func getSrvKeyspace(rpcClient *rpcplus.Client, cell, keyspace string, verbose bool) {
	req := &topo.GetSrvKeyspaceArgs{
		Cell:     cell,
		Keyspace: keyspace,
	}
	reply := &topo.SrvKeyspace{}
	if err := rpcClient.Call("TopoReader.GetSrvKeyspace", req, reply); err != nil {
		log.Fatalf("TopoReader.GetSrvKeyspace error: %v", err)
	}
	if verbose {
		tabletTypes := make([]string, 0, len(reply.Partitions))
		for t, _ := range reply.Partitions {
			tabletTypes = append(tabletTypes, string(t))
		}
		sort.Strings(tabletTypes)
		for _, t := range tabletTypes {
			println(fmt.Sprintf("Partitions[%v] =", t))
			for i, s := range reply.Partitions[topo.TabletType(t)].Shards {
				println(fmt.Sprintf("  Shards[%v]=%v", i, s.KeyRange.String()))
			}
		}
		for i, s := range reply.Shards {
			println(fmt.Sprintf("Shards[%v]=%v", i, s.KeyRange.String()))
		}
		for i, t := range reply.TabletTypes {
			println(fmt.Sprintf("TabletTypes[%v] = %v", i, t))
		}
	}
}

func getEndPoints(rpcClient *rpcplus.Client, cell, keyspace, shard, tabletType string, verbose bool) {
	req := &topo.GetEndPointsArgs{
		Cell:       cell,
		Keyspace:   keyspace,
		Shard:      shard,
		TabletType: topo.TabletType(tabletType),
	}
	reply := &topo.EndPoints{}
	if err := rpcClient.Call("TopoReader.GetEndPoints", req, reply); err != nil {
		log.Fatalf("TopoReader.GetEndPoints error: %v", err)
	}
	if verbose {
		for i, e := range reply.Entries {
			println(fmt.Sprintf("Entries[%v] = %v %v", i, e.Uid, e.Host))
		}
	}
}

// qps is a function used by tests to run a zkocc load check.
// It will get zk paths as fast as possible and display the QPS.
func qps(paths []string) {
	var count sync2.AtomicInt32
	for _, path := range paths {
		for i := 0; i < 100; i++ {
			go func() {
				rpcClient := connect()
				for true {
					get(rpcClient, path, false)
					count.Add(1)
				}
			}()
		}
	}

	ticker := time.NewTicker(time.Second)
	for _ = range ticker.C {
		c := count.Get()
		count.Set(0)
		println(fmt.Sprintf("QPS = %v", c))
	}
}

// qps2 is a function used by tests to run a vtgate load check.
// It will get the same srvKeyspaces as fast as possible and display the QPS.
func qps2(cell string, keyspaces []string) {
	var count sync2.AtomicInt32
	for _, keyspace := range keyspaces {
		for i := 0; i < 100; i++ {
			go func() {
				rpcClient := connect()
				for true {
					getSrvKeyspace(rpcClient, cell, keyspace, false)
					count.Add(1)
				}
			}()
		}
	}

	ticker := time.NewTicker(time.Second)
	for _ = range ticker.C {
		c := count.Get()
		count.Set(0)
		println(fmt.Sprintf("QPS = %v", c))
	}
}

func main() {
	defer logutil.Flush()

	flag.Parse()
	args := flag.Args()
	if len(args) == 0 {
		flag.Usage()
		os.Exit(1)
	}

	if *mode == "get" {
		rpcClient := connect()
		if len(args) == 1 {
			get(rpcClient, args[0], true)
		} else {
			getv(rpcClient, args, true)
		}

	} else if *mode == "children" {
		rpcClient := connect()
		children(rpcClient, args, true)

	} else if *mode == "getSrvKeyspaceNames" {
		rpcClient := connect()
		if len(args) == 1 {
			getSrvKeyspaceNames(rpcClient, args[0], true)
		} else {
			log.Fatalf("getSrvKeyspaceNames only takes one argument")
		}

	} else if *mode == "getSrvKeyspace" {
		rpcClient := connect()
		if len(args) == 2 {
			getSrvKeyspace(rpcClient, args[0], args[1], true)
		} else {
			log.Fatalf("getSrvKeyspace only takes two arguments")
		}

	} else if *mode == "getEndPoints" {
		rpcClient := connect()
		if len(args) == 4 {
			getEndPoints(rpcClient, args[0], args[1], args[2], args[3], true)
		} else {
			log.Fatalf("getEndPoints only takes four arguments")
		}

	} else if *mode == "qps" {
		qps(args)

	} else if *mode == "qps2" {
		qps2(args[0], args[1:])

	} else {
		flag.Usage()
		log.Fatalf("Invalid mode: %v", *mode)
	}
}
