// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package main

import (
	"flag"
	"fmt"
	"os"
	"runtime/pprof"
	"sort"
	"time"

	"golang.org/x/net/context"

	log "github.com/golang/glog"
	"github.com/youtube/vitess/go/exit"
	"github.com/youtube/vitess/go/rpcplus"
	"github.com/youtube/vitess/go/rpcwrap/bsonrpc"
	"github.com/youtube/vitess/go/sync2"
	"github.com/youtube/vitess/go/vt/logutil"
	"github.com/youtube/vitess/go/vt/topo"
)

var (
	usage = `
Queries the topo server, for test purposes.
`
	mode       = flag.String("mode", "get", "which operation to run on the node (getSrvKeyspaceNames, getSrvKeyspace, getEndPoints, qps)")
	server     = flag.String("server", "localhost:3801", "topo server to dial")
	timeout    = flag.Duration("timeout", 5*time.Second, "connection timeout")
	cpuProfile = flag.String("cpu_profile", "", "write cpu profile to file")
)

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
		log.Fatalf("Can't connect to topo server: %v", err)
	}
	return rpcClient
}

func getSrvKeyspaceNames(rpcClient *rpcplus.Client, cell string, verbose bool) {
	req := &topo.GetSrvKeyspaceNamesArgs{
		Cell: cell,
	}
	reply := &topo.SrvKeyspaceNames{}
	if err := rpcClient.Call(context.TODO(), "TopoReader.GetSrvKeyspaceNames", req, reply); err != nil {
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
	if err := rpcClient.Call(context.TODO(), "TopoReader.GetSrvKeyspace", req, reply); err != nil {
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
	if err := rpcClient.Call(context.TODO(), "TopoReader.GetEndPoints", req, reply); err != nil {
		log.Fatalf("TopoReader.GetEndPoints error: %v", err)
	}
	if verbose {
		for i, e := range reply.Entries {
			println(fmt.Sprintf("Entries[%v] = %v %v", i, e.Uid, e.Host))
		}
	}
}

// qps is a function used by tests to run a vtgate load check.
// It will get the same srvKeyspaces as fast as possible and display the QPS.
func qps(cell string, keyspaces []string) {
	var count sync2.AtomicInt32
	for _, keyspace := range keyspaces {
		for i := 0; i < 10; i++ {
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
	i := 0
	for _ = range ticker.C {
		c := count.Get()
		count.Set(0)
		println(fmt.Sprintf("QPS = %v", c))
		i++
		if i == 10 {
			break
		}
	}
}

func main() {
	defer exit.Recover()
	defer logutil.Flush()

	flag.Parse()
	args := flag.Args()
	if len(args) == 0 {
		flag.Usage()
		exit.Return(1)
	}

	if *cpuProfile != "" {
		f, err := os.Create(*cpuProfile)
		if err != nil {
			log.Error(err)
			exit.Return(1)
		}
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}

	if *mode == "getSrvKeyspaceNames" {
		rpcClient := connect()
		if len(args) == 1 {
			getSrvKeyspaceNames(rpcClient, args[0], true)
		} else {
			log.Errorf("getSrvKeyspaceNames only takes one argument")
			exit.Return(1)
		}

	} else if *mode == "getSrvKeyspace" {
		rpcClient := connect()
		if len(args) == 2 {
			getSrvKeyspace(rpcClient, args[0], args[1], true)
		} else {
			log.Errorf("getSrvKeyspace only takes two arguments")
			exit.Return(1)
		}

	} else if *mode == "getEndPoints" {
		rpcClient := connect()
		if len(args) == 4 {
			getEndPoints(rpcClient, args[0], args[1], args[2], args[3], true)
		} else {
			log.Errorf("getEndPoints only takes four arguments")
			exit.Return(1)
		}

	} else if *mode == "qps" {
		qps(args[0], args[1:])

	} else {
		flag.Usage()
		log.Errorf("Invalid mode: %v", *mode)
		exit.Return(1)
	}
}
