/*
Copyright 2017 Google Inc.

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

// Package dtids contains dtid convenience functions.
package dtids

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/youtube/vitess/go/vt/vterrors"

	querypb "github.com/youtube/vitess/go/vt/proto/query"
	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
	vtgatepb "github.com/youtube/vitess/go/vt/proto/vtgate"
	vtrpcpb "github.com/youtube/vitess/go/vt/proto/vtrpc"
)

// New generates a dtid based on Session_ShardSession.
func New(mmShard *vtgatepb.Session_ShardSession) string {
	return fmt.Sprintf("%s:%s:%d", mmShard.Target.Keyspace, mmShard.Target.Shard, mmShard.TransactionId)
}

// ShardSession builds a Session_ShardSession from a dtid.
func ShardSession(dtid string) (*vtgatepb.Session_ShardSession, error) {
	splits := strings.Split(dtid, ":")
	if len(splits) != 3 {
		return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "invalid parts in dtid: %s", dtid)
	}
	target := &querypb.Target{
		Keyspace:   splits[0],
		Shard:      splits[1],
		TabletType: topodatapb.TabletType_MASTER,
	}
	txid, err := strconv.ParseInt(splits[2], 10, 0)
	if err != nil {
		return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "invalid transaction id in dtid: %s", dtid)
	}
	return &vtgatepb.Session_ShardSession{
		Target:        target,
		TransactionId: txid,
	}, nil
}

// TransactionID extracts the original transaction ID from the dtid.
func TransactionID(dtid string) (int64, error) {
	splits := strings.Split(dtid, ":")
	if len(splits) != 3 {
		return 0, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "invalid parts in dtid: %s", dtid)
	}
	txid, err := strconv.ParseInt(splits[2], 10, 0)
	if err != nil {
		return 0, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "invalid transaction id in dtid: %s", dtid)
	}
	return txid, nil
}
