/*
Copyright 2018 The Vitess Authors.

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

package vstreamer

import (
	"context"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/vt/binlog"
	"vitess.io/vitess/go/vt/vtgate/vindexes"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
)

type vstreamer struct {
	cp       *mysql.ConnParams
	startPos mysql.Position
	filter   *binlogdatapb.Filter
	f        func()
	conn     *binlog.SlaveConnection
}

func newVStreamer(cp *mysql.ConnParams, startPos mysql.Position, filter *binlogdatapb.Filter, f func()) (*vstreamer, error) {
	return &vstreamer{
		cp:       cp,
		startPos: startPos,
		filter:   filter,
		f:        f,
	}, nil
}

func (vs *vstreamer) SetKSchema(kschema *vindexes.KeyspaceSchema) {
}

func (vs *vstreamer) Stream(ctx context.Context) error {
	return nil
}

func (vs *vstreamer) Cancel() {
}
