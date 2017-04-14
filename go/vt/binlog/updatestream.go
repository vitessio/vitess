// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package binlog

import (
	"golang.org/x/net/context"

	binlogdatapb "github.com/youtube/vitess/go/vt/proto/binlogdata"
	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
)

// UpdateStream is the interface for the binlog server
type UpdateStream interface {
	// StreamKeyRange streams events related to a KeyRange only
	StreamKeyRange(ctx context.Context, position string, keyRange *topodatapb.KeyRange, charset *binlogdatapb.Charset, callback func(trans *binlogdatapb.BinlogTransaction) error) error

	// StreamTables streams events related to a set of Tables only
	StreamTables(ctx context.Context, position string, tables []string, charset *binlogdatapb.Charset, callback func(trans *binlogdatapb.BinlogTransaction) error) error

	// HandlePanic should be called in a defer,
	// first thing in the RPC implementation.
	HandlePanic(*error)
}
