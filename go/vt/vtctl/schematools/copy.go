/*
Copyright 2021 The Vitess Authors.

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

package schematools

import (
	"bytes"
	"context"
	"fmt"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/vttablet/tmclient"

	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

// CopyShardMetadata copies the contents of the _vt.shard_metadata table from
// the source tablet to the destination tablet.
//
// NOTE: This function assumes that the destination tablet is a primary with
// binary logging enabled, in order to propagate the INSERT statements to any
// replicas in the destination shard.
func CopyShardMetadata(ctx context.Context, ts *topo.Server, tmc tmclient.TabletManagerClient, source *topodatapb.TabletAlias, dest *topodatapb.TabletAlias) error {
	sourceTablet, err := ts.GetTablet(ctx, source)
	if err != nil {
		return fmt.Errorf("GetTablet(%v) failed: %w", topoproto.TabletAliasString(source), err)
	}

	destTablet, err := ts.GetTablet(ctx, dest)
	if err != nil {
		return fmt.Errorf("GetTablet(%v) failed: %w", topoproto.TabletAliasString(dest), err)
	}

	sql := "SELECT 1 FROM information_schema.tables WHERE table_schema = '_vt' AND table_name = 'shard_metadata'"
	presenceResult, err := tmc.ExecuteFetchAsDba(ctx, sourceTablet.Tablet, false, &tabletmanagerdatapb.ExecuteFetchAsDbaRequest{
		Query:   []byte(sql),
		MaxRows: 1,
	})
	if err != nil {
		return fmt.Errorf("ExecuteFetchAsDba(%v, false, %v, 1, false, false) failed: %v", topoproto.TabletAliasString(source), sql, err)
	}
	if len(presenceResult.Rows) == 0 {
		log.Infof("_vt.shard_metadata doesn't exist on the source tablet %v, skipping its copy.", topoproto.TabletAliasString(source))
		return nil
	}

	// (TODO|@ajm188,@deepthi): 100 may be too low here for row limit
	sql = "SELECT db_name, name, value FROM _vt.shard_metadata"
	p3qr, err := tmc.ExecuteFetchAsDba(ctx, sourceTablet.Tablet, false, &tabletmanagerdatapb.ExecuteFetchAsDbaRequest{
		Query:   []byte(sql),
		MaxRows: 100,
	})
	if err != nil {
		return fmt.Errorf("ExecuteFetchAsDba(%v, false, %v, 100, false, false) failed: %v", topoproto.TabletAliasString(source), sql, err)
	}

	qr := sqltypes.Proto3ToResult(p3qr)
	queryBuf := bytes.NewBuffer(nil)
	for _, row := range qr.Rows {
		dbName := row[0]
		name := row[1]
		value := row[2]
		queryBuf.WriteString("INSERT INTO _vt.shard_metadata (db_name, name, value) VALUES (")
		dbName.EncodeSQL(queryBuf)
		queryBuf.WriteByte(',')
		name.EncodeSQL(queryBuf)
		queryBuf.WriteByte(',')
		value.EncodeSQL(queryBuf)
		queryBuf.WriteString(") ON DUPLICATE KEY UPDATE value = ")
		value.EncodeSQL(queryBuf)

		_, err := tmc.ExecuteFetchAsDba(ctx, destTablet.Tablet, false, &tabletmanagerdatapb.ExecuteFetchAsDbaRequest{
			Query:   queryBuf.Bytes(),
			MaxRows: 0,
		})
		if err != nil {
			return fmt.Errorf("ExecuteFetchAsDba(%v, false, %v, 0, false, false) failed: %v", topoproto.TabletAliasString(dest), queryBuf.String(), err)
		}

		queryBuf.Reset()
	}

	return nil
}
