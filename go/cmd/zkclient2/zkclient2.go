// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"code.google.com/p/vitess/go/rpcplus"
	"code.google.com/p/vitess/go/rpcwrap/bsonrpc"
	"code.google.com/p/vitess/go/sync2"
	"code.google.com/p/vitess/go/zk"
)

var usage = `
Queries the zkocc zookeeper cache, for test purposes. In get mode, if more
than one value is asked for, will use getv.
`
var mode = flag.String("mode", "get", "which operation to run on the node (get, children, qps)")
var server = flag.String("server", "localhost:3801", "zkocc server to dial")

func init() {
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage of %s:\n", os.Args[0])
		flag.PrintDefaults()
		fmt.Fprintf(os.Stderr, usage)
	}
}

func connect() *rpcplus.Client {
	rpcClient, err := bsonrpc.DialHTTP("tcp", *server, 0)
	if err != nil {
		log.Fatalf("Can't connect to zkocc: %v", err)
	}
	return rpcClient
}

func get(rpcClient *rpcplus.Client, path string, verbose bool) {
	// it's a get
	zkPath := &zk.ZkPath{path}
	zkNode := &zk.ZkNode{}
	if err := rpcClient.Call("ZkReader.Get", zkPath, zkNode); err != nil {
		log.Fatalf("ZkReader.Get error: %v", err)
	}
	if verbose {
		println(fmt.Sprintf("%v = %v (NumChildren=%v, Version=%v, Cached=%v, Stale=%v)", zkNode.Path, zkNode.Data, zkNode.Stat.NumChildren(), zkNode.Stat.Version(), zkNode.Cached, zkNode.Stale))
	}

}

func getv(rpcClient *rpcplus.Client, paths []string, verbose bool) {
	zkPathV := &zk.ZkPathV{make([]string, len(paths))}
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
		zkPath := &zk.ZkPath{v}
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

func main() {
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

	} else if *mode == "qps" {
		qps(args)

	} else {
		flag.Usage()
		log.Fatalf("Invalid mode: %v", mode)
	}
}
