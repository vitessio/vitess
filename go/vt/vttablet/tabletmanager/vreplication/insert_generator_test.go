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
		`('b', 'keyspace:\"c\"', 'd', 9223372036854775807, 9223372036854775807, 'e', 'f', 111, 0, 'Stopped', 'a')`
	assert.Equal(t, ig.String(), want)

	ig.AddRow("g", &binlogdatapb.BinlogSource{Keyspace: "h"}, "i", "j", "k")
	want += `, ('g', 'keyspace:\"h\"', 'i', 9223372036854775807, 9223372036854775807, 'j', 'k', 111, 0, 'Stopped', 'a')`
	assert.Equal(t, ig.String(), want)
}
