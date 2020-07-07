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
