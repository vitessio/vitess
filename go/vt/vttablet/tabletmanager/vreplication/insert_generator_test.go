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

package vreplication

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"vitess.io/vitess/go/vt/binlog/binlogplayer"
	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
)

func TestInsertGenerator(t *testing.T) {
	ig := NewInsertGenerator(binlogplayer.BlpStopped, "a")
	ig.now = 111
	ig.AddRow("b", &binlogdatapb.BinlogSource{Keyspace: "c"}, "d", "e", "f")
	want := `insert into _vt.vreplication(workflow, source, pos, max_tps, max_replication_lag, cell, tablet_types, time_updated, transaction_timestamp, state, db_name) values ` +
		`('b', 'keyspace:\"c\" ', 'd', 9223372036854775807, 9223372036854775807, 'e', 'f', 111, 0, 'Stopped', 'a')`
	assert.Equal(t, ig.String(), want)

	ig.AddRow("g", &binlogdatapb.BinlogSource{Keyspace: "h"}, "i", "j", "k")
	want += `, ('g', 'keyspace:\"h\" ', 'i', 9223372036854775807, 9223372036854775807, 'j', 'k', 111, 0, 'Stopped', 'a')`
	assert.Equal(t, ig.String(), want)
}
