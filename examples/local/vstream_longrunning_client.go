/*
Copyright 2021 The Vitess Authors.

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

package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"os/exec"
	"time"
	"vitess.io/vitess/go/sync2"
	vtgatepb "vitess.io/vitess/go/vt/proto/vtgate"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	_ "vitess.io/vitess/go/vt/vtctl/grpcvtctlclient"
	_ "vitess.io/vitess/go/vt/vtgate/grpcvtgateconn"
	"vitess.io/vitess/go/vt/vtgate/vtgateconn"
)

/*
	This is a sample client for streaming using the vstream API. It is setup to work with the local example and you can
    either stream from the unsharded commerce keyspace or the customer keyspace after the sharding step.
*/

var gvgtid *binlogdatapb.VGtid

func main() {
	gvgtid = &binlogdatapb.VGtid{
		ShardGtids: []*binlogdatapb.ShardGtid{{
			Keyspace: "customer",
			Shard:    "-80",
			Gtid:     "",
		},{
			Keyspace: "customer",
			Shard:    "80-",
			Gtid:     "",
		}},
	}
	filter := &binlogdatapb.Filter{
		Rules: []*binlogdatapb.Rule{{
			Match:  "customer",
			Filter: "select * from customer",
		}},
	}
	go func() {
		for {
			cmd := exec.Command("mysql", "-u", "root", "--host","127.0.0.1", "-P", "15306", "-e", "insert into customer(email,lt) values('a',repeat('a', 100000)),('a',repeat('a', 100000)),('a',repeat('a', 100000)),('a',repeat('a', 100000)),('a',repeat('a', 100000)),('a',repeat('a', 100000)),('a',repeat('a', 100000)),('a',repeat('a', 100000)),('a',repeat('a', 100000)),('a',repeat('a', 100000)),('a',repeat('a', 100000)),('a',repeat('a', 100000)),('a',repeat('a', 100000)),('a',repeat('a', 100000)),('a',repeat('a', 100000)),('a',repeat('a', 100000)),('a',repeat('a', 100000)),('a',repeat('a', 100000)),('a',repeat('a', 100000)),('a',repeat('a', 100000)),('a',repeat('a', 100000)),('a',repeat('a', 100000)),('a',repeat('a', 100000)),('a',repeat('a', 100000)),('a',repeat('a', 100000)),('a',repeat('a', 100000)),('a',repeat('a', 100000)),('a',repeat('a', 100000)),('a',repeat('a', 100000)),('a',repeat('a', 100000)),('a',repeat('a', 100000)),('a',repeat('a', 100000)),('a',repeat('a', 100000)),('a',repeat('a', 100000)),('a',repeat('a', 100000)),('a',repeat('a', 100000)),('a',repeat('a', 100000)),('a',repeat('a', 100000)),('a',repeat('a', 100000)),('a',repeat('a', 100000)),('a',repeat('a', 100000)),('a',repeat('a', 100000)),('a',repeat('a', 100000)),('a',repeat('a', 100000))")
			o, err := cmd.Output()
			if err != nil {
				panic(o)
			}
			time.Sleep(1 * time.Millisecond)
		}
	}()
	ticker := time.NewTicker(20 * time.Minute)
	for {
		log.Printf("#Active Counters: %d", ctr.Get())
		ctx2, cancel := context.WithCancel(context.Background())
		go func() {
			ctr.Add(1)
			defer func() {
				ctr.Add(-1)
			}()
			log.Printf("Streaming from %+v\n", gvgtid)
			stream(ctx2, filter, gvgtid)
		}()
		select {
		case <-ticker.C:
			cancel()
		}
	}
}
var ctr sync2.AtomicInt64

func stream(ctx context.Context, filter *binlogdatapb.Filter, vgtid *binlogdatapb.VGtid) {
	conn, err := vtgateconn.Dial(ctx, "localhost:15991")
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()
	reader, err := conn.VStream(ctx, topodatapb.TabletType_PRIMARY, vgtid, filter, &vtgatepb.VStreamFlags{})
	for {
		evs, err := reader.Recv()
		switch err {
		case nil:
			_ = evs
			for _, ev := range evs {
				if ev.Vgtid != nil {
					gvgtid = ev.Vgtid
					//log.Printf("Setting vgtid as %+v", ev.Vgtid)
				}
			}
		case io.EOF:
			fmt.Printf("\nstream ended\n")
			return
		default:
			fmt.Printf("\n%s:: remote error: %v\n", time.Now(), err)
			return
		}
	}
}
