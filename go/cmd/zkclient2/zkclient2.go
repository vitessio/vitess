// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package main

import (
	"flag"
	"fmt"
	"log"
	"os"

	"code.google.com/p/vitess/go/rpcwrap/bsonrpc"
	"code.google.com/p/vitess/go/zk/zkocc/proto"
)

var usage = `
Queries the zkocc zookeeper cache, for test purposes. In get mode, if more
than one value is asked for, will use getv.
`
var mode = flag.String("mode", "get", "which operation to run on the node (get, children)")
var server = flag.String("server", "localhost:3801", "zkocc server to dial")

func init() {
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage of %s:\n", os.Args[0])
		flag.PrintDefaults()
		fmt.Fprintf(os.Stderr, usage)
	}
}

func main() {
	flag.Parse()
	args := flag.Args()

	if len(args) == 0 {
		flag.Usage()
		os.Exit(1)
	}

	// connect to the RPC server using bson
	rpcClient, err := bsonrpc.DialHTTP("tcp", *server)
	if err != nil {
		log.Fatalf("Can't connect to zkocc: %v", err)
	}

	if *mode == "get" {
		if len(args) == 1 {
			// it's a get
			zkPath := &proto.ZkPath{args[0]}
			zkNode := &proto.ZkNode{}
			if err := rpcClient.Call("ZkReader.Get", zkPath, zkNode); err != nil {
				log.Fatalf("ZkReader.Get error: %v", err)
			}
			println(fmt.Sprintf("%v = %v (NumChildren=%v, Version=%v, Cached=%v, Stale=%v)", zkNode.Path, zkNode.Data, zkNode.Stat.NumChildren, zkNode.Stat.Version, zkNode.Cached, zkNode.Stale))
		} else {
			// it's a getv
			zkPathV := &proto.ZkPathV{make([]string, len(args))}
			for i, v := range args {
				zkPathV.Paths[i] = v
			}
			zkNodeV := &proto.ZkNodeV{}
			if err := rpcClient.Call("ZkReader.GetV", zkPathV, zkNodeV); err != nil {
				log.Fatalf("ZkReader.GetV error: %v", err)
			}
			for i, zkNode := range zkNodeV.Nodes {
				println(fmt.Sprintf("[%v] %v = %v (NumChildren=%v, Version=%v, Cached=%v, Stale=%v)", i, zkNode.Path, zkNode.Data, zkNode.Stat.NumChildren, zkNode.Stat.Version, zkNode.Cached, zkNode.Stale))
			}
		}
	} else if *mode == "children" {
		for _, v := range args {
			zkPath := &proto.ZkPath{v}
			zkNode := &proto.ZkNode{}
			if err := rpcClient.Call("ZkReader.Children", zkPath, zkNode); err != nil {
				log.Fatalf("ZkReader.Children error: %v", err)
			}
			println(fmt.Sprintf("Path = %v", zkNode.Path))
			for i, child := range zkNode.Children {
				println(fmt.Sprintf("Child[%v] = %v", i, child))
			}
			println(fmt.Sprintf("NumChildren = %v", zkNode.Stat.NumChildren))
			println(fmt.Sprintf("CVersion = %v", zkNode.Stat.CVersion))
			println(fmt.Sprintf("Cached = %v", zkNode.Cached))
			println(fmt.Sprintf("Stale = %v", zkNode.Stale))
		}
	} else {
		flag.Usage()
		log.Fatalf("Invalid mode: %v", mode)
	}
}
