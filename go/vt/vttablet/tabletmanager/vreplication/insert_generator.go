package vreplication

import (
	"fmt"
	"strings"
	"time"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	"vitess.io/vitess/go/vt/throttler"
)

// InsertGenerator generates a vreplication insert statement.
type InsertGenerator struct {
	buf    *strings.Builder
	prefix string

	state  string
	dbname string
	now    int64
}

// NewInsertGenerator creates a new InsertGenerator.
func NewInsertGenerator(state, dbname string) *InsertGenerator {
	buf := &strings.Builder{}
	buf.WriteString("insert into _vt.vreplication(workflow, source, pos, max_tps, max_replication_lag, cell, tablet_types, time_updated, transaction_timestamp, state, db_name) values ")
	return &InsertGenerator{
		buf:    buf,
		state:  state,
		dbname: dbname,
		now:    time.Now().Unix(),
	}
}

// AddRow adds a row to the insert statement.
func (ig *InsertGenerator) AddRow(workflow string, bls *binlogdatapb.BinlogSource, pos, cell, tabletTypes string) {
	fmt.Fprintf(ig.buf, "%s(%v, %v, %v, %v, %v, %v, %v, %v, 0, '%v', %v)",
		ig.prefix,
		encodeString(workflow),
		encodeString(bls.String()),
		encodeString(pos),
		throttler.MaxRateModuleDisabled,
		throttler.ReplicationLagModuleDisabled,
		encodeString(cell),
		encodeString(tabletTypes),
		ig.now,
		ig.state,
		encodeString(ig.dbname),
	)
	ig.prefix = ", "
}

// String returns the generated statement.
func (ig *InsertGenerator) String() string {
	return ig.buf.String()
}
