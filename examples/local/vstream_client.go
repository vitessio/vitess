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
	"time"

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
func main() {
	ctx := context.Background()
	streamCustomer := true
	var vgtid *binlogdatapb.VGtid
	if streamCustomer {
		vgtid = &binlogdatapb.VGtid{
			ShardGtids: []*binlogdatapb.ShardGtid{{
				Keyspace: "customer",
				Shard:    "-80",
				// Gtid "" is to stream from the start, "current" is to stream from the current gtid
				// you can also specify a gtid to start with.
				Gtid: "", //"current"  // "MySQL56/36a89abd-978f-11eb-b312-04ed332e05c2:1-265"
			}, {
				Keyspace: "customer",
				Shard:    "80-",
				Gtid:     "",
			}}}
	} else {
		vgtid = &binlogdatapb.VGtid{
			ShardGtids: []*binlogdatapb.ShardGtid{{
				Keyspace: "commerce",
				Shard:    "0",
				Gtid:     "",
			}}}
	}
	filter := &binlogdatapb.Filter{
		Rules: []*binlogdatapb.Rule{{
			Match:  "customer",
			Filter: "select * from customer",
		}},
	}
	conn, err := vtgateconn.Dial(ctx, "localhost:15991")
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()
	flags := &vtgatepb.VStreamFlags{
		//MinimizeSkew:      false,
		//HeartbeatInterval: 60, //seconds
	}
	reader, err := conn.VStream(ctx, topodatapb.TabletType_PRIMARY, vgtid, filter, flags)
	for {
		e, err := reader.Recv()
		switch err {
		case nil:
			_ = e
			fmt.Printf("%v\n", e)
		case io.EOF:
			fmt.Printf("stream ended\n")
			return
		default:
			fmt.Printf("%s:: remote error: %v\n", time.Now(), err)
			return
		}
	}
}
