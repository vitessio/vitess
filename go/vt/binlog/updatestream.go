/*
Copyright 2019 The Vitess Authors.

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

package binlog

import (
	"golang.org/x/net/context"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
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
