// Copyright 2016, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

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
