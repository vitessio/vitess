/*
Copyright 2025 The Vitess Authors.

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

package vstreamclient

import (
	"context"
	"encoding/json"
	"fmt"

	"google.golang.org/protobuf/encoding/protojson"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	querypb "vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/vtgate/vtgateconn"
)

type dbTableConfig struct {
	Keyspace string
	Table    string
	Query    string
}

func tablesToDBTableConfig(tables map[string]*TableConfig) map[string]dbTableConfig {
	m := make(map[string]dbTableConfig, len(tables))

	for k, table := range tables {
		m[k] = dbTableConfig{
			Keyspace: table.Keyspace,
			Table:    table.Table,
			Query:    table.Query,
		}
	}

	return m
}

func initStateTable(ctx context.Context, session *vtgateconn.VTGateSession, keyspaceName, tableName string) error {
	query := fmt.Sprintf(`create table if not exists %s.%s (
  name varbinary(64) not null,
  latest_vgtid json,
  table_config json not null,
  copy_completed bool not null default false,
  created_at timestamp default current_timestamp,
  updated_at timestamp default current_timestamp on update current_timestamp,
  PRIMARY KEY (name)
)`, keyspaceName, tableName)

	_, err := session.Execute(ctx, query, nil, false)
	if err != nil {
		return fmt.Errorf("vstreamclient: failed to create state table: %w", err)
	}

	return nil
}

// initVGtid is used when there is no existing state for the stream, which means we need to create a new vgtid that
// starts from the beginning of the stream (empty gtid for each shard) and persist that along with the table config.
// This will kick off a copy phase on the vttablet side, which will read all existing data and then transition to
// streaming new changes.
func initVGtid(ctx context.Context, session *vtgateconn.VTGateSession, name, keyspaceName, tableName string, tables map[string]*TableConfig, shardsByKeyspace map[string][]string) (*binlogdatapb.VGtid, error) {
	vgtid, err := newVGtid(tables, shardsByKeyspace)
	if err != nil {
		return nil, err
	}

	latestVgtidJSON, tablesJSON, err := marshalState(vgtid, tables)
	if err != nil {
		return nil, err
	}

	query := fmt.Sprintf(`insert into %s.%s (name, latest_vgtid, table_config) values (:name, :latest_vgtid, :table_config)
on duplicate key update latest_vgtid = values(latest_vgtid), table_config = values(table_config)`,
		keyspaceName, tableName,
	)
	_, err = session.Execute(ctx, query, map[string]*querypb.BindVariable{
		"name":         {Type: querypb.Type_VARBINARY, Value: []byte(name)},
		"latest_vgtid": {Type: querypb.Type_JSON, Value: latestVgtidJSON},
		"table_config": {Type: querypb.Type_JSON, Value: tablesJSON},
	}, false)
	if err != nil {
		return nil, fmt.Errorf("vstreamclient: failed to get latest vgtid for %s.%s: %w", keyspaceName, tableName, err)
	}

	return vgtid, nil
}

// initStartingVGtid is used when the caller explicitly provides a starting vgtid, which means we want to persist
// that vgtid and the table config. Since they provided a vgtid, their intention isn't to start a copy phase, so we set
// copy_completed to true, which means the stream will start from the provided vgtid and not attempt to do a copy phase,
// either now or on restart.
func initStartingVGtid(ctx context.Context, session *vtgateconn.VTGateSession, name, keyspaceName, tableName string, vgtid *binlogdatapb.VGtid, tables map[string]*TableConfig) error {
	latestVgtidJSON, tablesJSON, err := marshalState(vgtid, tables)
	if err != nil {
		return err
	}

	query := fmt.Sprintf(`insert into %s.%s (name, latest_vgtid, table_config, copy_completed) values (:name, :latest_vgtid, :table_config, true)
on duplicate key update latest_vgtid = values(latest_vgtid), table_config = values(table_config), copy_completed = true`,
		keyspaceName, tableName,
	)
	_, err = session.Execute(ctx, query, map[string]*querypb.BindVariable{
		"name":         {Type: querypb.Type_VARBINARY, Value: []byte(name)},
		"latest_vgtid": {Type: querypb.Type_JSON, Value: latestVgtidJSON},
		"table_config": {Type: querypb.Type_JSON, Value: tablesJSON},
	}, false)
	if err != nil {
		return fmt.Errorf("vstreamclient: failed to initialize starting vgtid for %s.%s: %w", keyspaceName, tableName, err)
	}

	return nil
}

func marshalState(vgtid *binlogdatapb.VGtid, tables map[string]*TableConfig) (latestVgtidJSON []byte, tablesJSON []byte, err error) {
	latestVgtidJSON, err = protojson.Marshal(vgtid)
	if err != nil {
		return nil, nil, fmt.Errorf("vstreamclient: failed to marshal latest vgtid: %w", err)
	}

	tablesJSON, err = json.Marshal(tablesToDBTableConfig(tables))
	if err != nil {
		return nil, nil, fmt.Errorf("vstreamclient: failed to marshal tables: %w", err)
	}

	return latestVgtidJSON, tablesJSON, nil
}

func newVGtid(tables map[string]*TableConfig, shardsByKeyspace map[string][]string) (*binlogdatapb.VGtid, error) {
	bootstrappedKeyspaces := make(map[string]bool)
	vgtid := &binlogdatapb.VGtid{}

	for _, table := range tables {
		if bootstrappedKeyspaces[table.Keyspace] {
			continue
		}

		// TODO: this currently doesn't support subsetting shards, but we can add that if needed
		shards, ok := shardsByKeyspace[table.Keyspace]
		if !ok {
			return nil, fmt.Errorf("vstreamclient: keyspace %s not found", table.Keyspace)
		}

		for _, shard := range shards {
			vgtid.ShardGtids = append(vgtid.ShardGtids, &binlogdatapb.ShardGtid{
				Keyspace: table.Keyspace,
				Shard:    shard,
				Gtid:     "", // start from the beginning, meaning initializing a copy phase
			})
		}
		bootstrappedKeyspaces[table.Keyspace] = true
	}

	return vgtid, nil
}

func getLatestVGtid(ctx context.Context, session *vtgateconn.VTGateSession, name, keyspaceName, tableName string) (*binlogdatapb.VGtid, map[string]*TableConfig, bool, error) {
	query := fmt.Sprintf(`select latest_vgtid, table_config, copy_completed from %s.%s where name = :name`, keyspaceName, tableName)

	result, err := session.Execute(ctx, query, map[string]*querypb.BindVariable{
		"name": {Type: querypb.Type_VARBINARY, Value: []byte(name)},
	}, false)
	if err != nil {
		return nil, nil, false, fmt.Errorf("vstreamclient: failed to get latest vgtid for %s.%s: %w", keyspaceName, tableName, err)
	}

	// if there are no rows, or the value is null, return nil, which will start the stream from the beginning
	if len(result.Rows) == 0 || result.Rows[0][0].IsNull() {
		return nil, nil, false, nil
	}

	// unmarshal the JSON value which should be a valid, initialized VGtid
	latestVGtidJSON, err := result.Rows[0][0].ToBytes()
	if err != nil {
		return nil, nil, false, fmt.Errorf("vstreamclient: failed to convert latest_vgtid to bytes: %w", err)
	}

	var latestVGtid binlogdatapb.VGtid
	err = protojson.Unmarshal(latestVGtidJSON, &latestVGtid)
	if err != nil {
		return nil, nil, false, fmt.Errorf("vstreamclient: failed to unmarshal latest_vgtid: %w", err)
	}

	// unmarshal the JSON value which should be the original table config
	tablesJSON, err := result.Rows[0][1].ToBytes()
	if err != nil {
		return nil, nil, false, fmt.Errorf("vstreamclient: failed to convert table_config to bytes: %w", err)
	}

	var tables map[string]*TableConfig
	err = json.Unmarshal(tablesJSON, &tables)
	if err != nil {
		return nil, nil, false, fmt.Errorf("vstreamclient: failed to unmarshal table_config: %w", err)
	}

	// check if the copy has been completed
	copyCompleted, err := result.Rows[0][2].ToBool()
	if err != nil {
		return nil, nil, false, fmt.Errorf("vstreamclient: failed to convert copy_completed to bool: %w", err)
	}

	return &latestVGtid, tables, copyCompleted, nil
}

func updateLatestVGtid(ctx context.Context, session *vtgateconn.VTGateSession, name, keyspaceName, tableName string, vgtid *binlogdatapb.VGtid, setCopyCompleted bool) error {
	latestVgtid, err := protojson.Marshal(vgtid)
	if err != nil {
		return fmt.Errorf("vstreamclient: failed to marshal latest_vgtid: %w", err)
	}

	query := fmt.Sprintf(`update %s.%s set latest_vgtid = :latest_vgtid where name = :name`, keyspaceName, tableName)
	if setCopyCompleted {
		query = fmt.Sprintf(`update %s.%s set latest_vgtid = :latest_vgtid, copy_completed = true where name = :name`, keyspaceName, tableName)
	}
	result, err := session.Execute(ctx, query, map[string]*querypb.BindVariable{
		"latest_vgtid": {Type: querypb.Type_JSON, Value: latestVgtid},
		"name":         {Type: querypb.Type_VARBINARY, Value: []byte(name)},
	}, false)
	if err != nil {
		return fmt.Errorf("vstreamclient: failed to update latest_vgtid for %s.%s: %w", keyspaceName, tableName, err)
	}

	if result.RowsAffected != 1 {
		return fmt.Errorf("vstreamclient: unexpected number of rows affected when setting latest_vgtid for %s.%s: %d", keyspaceName, tableName, result.RowsAffected)
	}

	return nil
}
