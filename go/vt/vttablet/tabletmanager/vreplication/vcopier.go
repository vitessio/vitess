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
	"bytes"
	"fmt"
	"strconv"
	"strings"

	"github.com/golang/protobuf/proto"
	"golang.org/x/net/context"
	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/binlog/binlogplayer"
	"vitess.io/vitess/go/vt/grpcclient"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vttablet/tabletconn"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	querypb "vitess.io/vitess/go/vt/proto/query"
)

type vcopier struct {
	vr    *vreplicator
	tplan *TablePlan
}

func newVCopier(vr *vreplicator) *vcopier {
	return &vcopier{
		vr: vr,
	}
}

func (vc *vcopier) initTablesForCopy(ctx context.Context) error {
	defer vc.vr.dbClient.Rollback()

	// Check if table exists.
	if _, err := vc.vr.dbClient.ExecuteFetch("select * from _vt.copy_state limit 1", 10); err != nil {
		// If it's a not found error, create it.
		merr, isSQLErr := err.(*mysql.SQLError)
		if !isSQLErr || !(merr.Num == mysql.ERNoSuchTable || merr.Num == mysql.ERBadDb) {
			return err
		}
		log.Info("Looks like _vt.copy_state table may not exist. Trying to create... ")
		for _, query := range CreateCopyState {
			if _, merr := vc.vr.dbClient.ExecuteFetch(query, 0); merr != nil {
				log.Errorf("Failed to ensure _vt.copy_state table exists: %v", merr)
				return err
			}
		}
	}
	if err := vc.vr.dbClient.Begin(); err != nil {
		return err
	}
	// Insert the table list only if at least one table matches.
	if len(vc.vr.pplan.TargetTables) != 0 {
		var buf strings.Builder
		buf.WriteString("insert into _vt.copy_state(vrepl_id, table_name) values ")
		prefix := ""
		for name := range vc.vr.pplan.TargetTables {
			fmt.Fprintf(&buf, "%s(%d, %s)", prefix, vc.vr.id, encodeString(name))
			prefix = ", "
		}
		if _, err := vc.vr.dbClient.ExecuteFetch(buf.String(), 1); err != nil {
			return err
		}
	}
	if err := vc.vr.setState(binlogplayer.VReplicationCopying, ""); err != nil {
		return err
	}
	return vc.vr.dbClient.Commit()
}

func (vc *vcopier) copyTables(ctx context.Context) error {
	for {
		qr, err := vc.vr.dbClient.ExecuteFetch(fmt.Sprintf("select table_name, lastpk from _vt.copy_state where vrepl_id=%d", vc.vr.id), 10000)
		if err != nil {
			return err
		}
		var inflight, inflightpk string
		for _, row := range qr.Rows {
			tableName := row[0].ToString()
			lastpk := row[1].ToString()
			// We have to copy either the first table or the one that's already inflight.
			// The first table is expected to be the one in-flight. But there's no need to check.
			if inflight == "" || lastpk != "" {
				inflight = tableName
				inflightpk = lastpk
			}
		}
		if inflight == "" {
			if err := vc.vr.setState(binlogplayer.BlpRunning, ""); err != nil {
				return err
			}
			return nil
		}
		if err := vc.copyTable(ctx, inflight, inflightpk); err != nil {
			return err
		}
	}
}

func (vc *vcopier) copyTable(ctx context.Context, tableName, lastpk string) error {
	defer vc.vr.dbClient.Rollback()

	log.Infof("Copying table %s, lastpk: %s", tableName, lastpk)

	initialPlan, ok := vc.vr.pplan.TargetTables[tableName]
	if !ok {
		return fmt.Errorf("plan not found for table: %s, curret plans are: %#v", tableName, vc.vr.pplan.TargetTables)
	}

	vsClient, err := tabletconn.GetDialer()(vc.vr.sourceTablet, grpcclient.FailFast(false))
	if err != nil {
		return fmt.Errorf("error dialing tablet: %v", err)
	}
	defer vsClient.Close(ctx)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	target := &querypb.Target{
		Keyspace:   vc.vr.sourceTablet.Keyspace,
		Shard:      vc.vr.sourceTablet.Shard,
		TabletType: vc.vr.sourceTablet.Type,
	}

	var lastpkqr *querypb.QueryResult
	if lastpk != "" {
		var r querypb.QueryResult
		if err := proto.UnmarshalText(lastpk, &r); err != nil {
			return err
		}
		if len(r.Rows) != 1 {
			return fmt.Errorf("unexpected value decoding lastpk: %v", r)
		}
		lastpkqr = &r
	}

	var pkfields []*querypb.Field
	var updateCopyState *sqlparser.ParsedQuery
	err = vsClient.VStreamRows(ctx, target, initialPlan.SendRule.Filter, lastpkqr, func(rows *binlogdatapb.VStreamRowsResponse) error {
		if vc.tplan == nil {
			if len(rows.Fields) == 0 {
				return fmt.Errorf("expecting field event first, got: %v", rows)
			}
			fieldEvent := &binlogdatapb.FieldEvent{
				TableName: tableName,
				Fields:    rows.Fields,
			}
			vc.tplan, err = vc.vr.buildExecutionPlan(fieldEvent)
			if err != nil {
				return err
			}
			pkfields = rows.Pkfields
			buf := sqlparser.NewTrackedBuffer(nil)
			buf.Myprintf("update _vt.copy_state set lastpk=%a where vrepl_id=%s and table_name=%s", ":lastpk", strconv.Itoa(int(vc.vr.id)), encodeString(tableName))
			updateCopyState = buf.ParsedQuery()
		}
		if len(rows.Rows) == 0 {
			return nil
		}
		query, err := vc.tplan.generateBulkInsert(rows)
		if err != nil {
			return err
		}
		var buf bytes.Buffer
		err = proto.CompactText(&buf, &querypb.QueryResult{
			Fields: pkfields,
			Rows:   []*querypb.Row{rows.Lastpk},
		})
		if err != nil {
			return err
		}
		bv := map[string]*querypb.BindVariable{
			"lastpk": {
				Type:  sqltypes.VarBinary,
				Value: buf.Bytes(),
			},
		}
		updateState, err := updateCopyState.GenerateQuery(bv, nil)
		if err != nil {
			return err
		}
		if err := vc.vr.dbClient.Begin(); err != nil {
			return err
		}
		if _, err := vc.vr.dbClient.ExecuteFetch(query, 0); err != nil {
			return err
		}
		if _, err := vc.vr.dbClient.ExecuteFetch(updateState, 0); err != nil {
			return err
		}
		if err := vc.vr.dbClient.Commit(); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return err
	}
	buf := sqlparser.NewTrackedBuffer(nil)
	buf.Myprintf("delete from _vt.copy_state where vrepl_id=%s and table_name=%s", strconv.Itoa(int(vc.vr.id)), encodeString(tableName))
	if _, err := vc.vr.dbClient.ExecuteFetch(buf.String(), 0); err != nil {
		return err
	}
	return nil
}
