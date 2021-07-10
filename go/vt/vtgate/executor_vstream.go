/*
Copyright 2020 The Vitess Authors.

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

package vtgate

import (
	"context"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/proto/binlogdata"
	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/srvtopo"
	"vitess.io/vitess/go/vt/vterrors"
)

func (e *Executor) handleVStream(ctx context.Context, sql string, target querypb.Target, callback func(*sqltypes.Result) error, vcursor *vcursorImpl, logStats *LogStats) error {
	stmt, err := sqlparser.Parse(sql)
	if err != nil {
		logStats.Error = err
		return err
	}
	vstreamStmt, ok := stmt.(*sqlparser.VStream)
	if !ok {
		logStats.Error = err
		return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "[BUG] unrecognized VSTREAM statement: %v", sql)
	}

	table, _, _, _, err := vcursor.FindTable(vstreamStmt.Table)
	if err != nil {
		logStats.Error = err
		return err
	}

	execStart := time.Now()
	logStats.PlanTime = execStart.Sub(logStats.StartTime)

	err = e.startVStream(ctx, table.Keyspace.Name, target.Shard, nil, vstreamStmt, callback)
	logStats.Error = err
	logStats.ExecuteTime = time.Since(execStart)
	return err
}

func getVStreamStartPos(stmt *sqlparser.VStream) (string, error) {
	var colName, pos string
	if stmt.Where != nil {
		switch v := stmt.Where.Expr.(type) {
		case *sqlparser.ComparisonExpr:
			if v.Operator == sqlparser.GreaterThanOp {
				switch c := v.Left.(type) {
				case *sqlparser.ColName:
					switch val := v.Right.(type) {
					case *sqlparser.Literal:
						pos = string(val.Val)
					}
					colName = strings.ToLower(c.Name.String())
					if colName != "pos" {
						return "", fmt.Errorf("can only use pos in vstream where clause ")
					}
				}
			} else {
				return "", fmt.Errorf("where can only be of type 'pos > <value>'")
			}
		default:
			return "", fmt.Errorf("where can only be of type 'pos > <value>'")
		}
	}
	return pos, nil
}

func (e *Executor) startVStream(ctx context.Context, keyspace string, shard string, keyRange *topodatapb.KeyRange, stmt *sqlparser.VStream, callback func(*sqltypes.Result) error) error {
	tableName := stmt.Table.Name.CompliantName()
	var pos string
	var err error
	gw := NewTabletGateway(ctx, vtgateHealthCheck /*discovery.Healthcheck*/, e.serv, e.cell)

	srvResolver := srvtopo.NewResolver(e.serv, gw, e.cell)

	limit := 100
	if stmt.Where != nil {
		pos, err = getVStreamStartPos(stmt)
		if err != nil {
			return err
		}
	}
	if stmt.Limit != nil {
		count, ok := stmt.Limit.Rowcount.(*sqlparser.Literal)
		if ok {
			limit, _ = strconv.Atoi(string(count.Val))
		}
	}
	log.Infof("startVStream for %s.%s.%s, position %s, limit %d", keyspace, shard, tableName, pos, limit)
	var shardGtids []*binlogdata.ShardGtid
	if shard != "" {
		shardGtid := &binlogdata.ShardGtid{
			Keyspace: keyspace,
			Shard:    shard,
			Gtid:     pos,
		}
		shardGtids = append(shardGtids, shardGtid)
	} else {
		_, _, shards, err := srvResolver.GetKeyspaceShards(ctx, keyspace, topodatapb.TabletType_MASTER)
		if err != nil {
			return err
		}
		for _, shard := range shards {
			shardGtid := &binlogdata.ShardGtid{
				Keyspace: keyspace,
				Shard:    shard.Name,
				Gtid:     pos,
			}
			shardGtids = append(shardGtids, shardGtid)
		}
	}
	vgtid := &binlogdata.VGtid{
		ShardGtids: shardGtids,
	}
	filter := &binlogdata.Filter{
		Rules: []*binlogdata.Rule{{
			Match:  tableName,
			Filter: fmt.Sprintf("select * from %s", tableName),
		}},
	}
	var lastFields []*querypb.Field
	numRows := 0
	totalRows := 0
	if limit == 0 {
		return io.EOF
	}
	send := func(evs []*binlogdata.VEvent) error {
		result := &sqltypes.Result{
			Fields: nil,
			Rows:   [][]sqltypes.Value{},
		}
		for _, ev := range evs {
			if totalRows+numRows >= limit {
				break
			}
			switch ev.Type {
			case binlogdata.VEventType_FIELD:
				lastFields = []*querypb.Field{{
					Name: "op",
					Type: querypb.Type_VARCHAR,
				}}
				lastFields = append(lastFields, ev.FieldEvent.Fields...)
			case binlogdata.VEventType_ROW:
				result.Fields = lastFields
				eventFields := lastFields[1:]
				for _, change := range ev.RowEvent.RowChanges {
					op := ""
					var vals []sqltypes.Value
					if change.After != nil && change.Before == nil {
						op = "+"
						vals = sqltypes.MakeRowTrusted(eventFields, change.After)
					} else if change.After != nil && change.Before != nil {
						op = "*"
						vals = sqltypes.MakeRowTrusted(eventFields, change.After)
					} else {
						op = "-"
						vals = sqltypes.MakeRowTrusted(eventFields, change.Before)
					}
					newVals := append([]sqltypes.Value{sqltypes.NewVarChar(op)}, vals...)
					result.Rows = append(result.Rows, newVals)
					numRows++
					if totalRows+numRows >= limit {
						break
					}
				}
			default:
			}
		}
		if numRows > 0 {
			err := callback(result)
			totalRows += numRows
			numRows = 0
			if err != nil {
				return err
			}
			if totalRows >= limit {
				return io.EOF
			}
		}
		return nil
	}

	vs := &vstream{
		vgtid:      vgtid,
		tabletType: topodatapb.TabletType_MASTER,
		filter:     filter,
		send:       send,
		resolver:   srvResolver,
	}
	vs.stream(ctx)
	return nil
}
