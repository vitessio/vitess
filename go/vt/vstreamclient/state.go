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
	"errors"
	"fmt"

	"google.golang.org/protobuf/encoding/protojson"

	"vitess.io/vitess/go/mysql/sqlerror"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	querypb "vitess.io/vitess/go/vt/proto/query"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/vtgateconn"
)

// ErrFenced is returned (as the wrapped cause) when a checkpoint write affects no rows, meaning the
// state row was deleted or another client using the same stream name claimed ownership after this
// client started. The newest client wins; this client must stop so the two don't ping-pong the
// checkpoint back and forth.
var ErrFenced = vterrors.New(vtrpcpb.Code_ABORTED, "vstreamclient: state row missing or claimed by another client")

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
  owner_token varbinary(36),
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

// insertStateRow creates the state row for a stream that has none. A plain insert (rather than an
// upsert) means a concurrent initializer surfaces as a duplicate-key conflict instead of silently
// stealing ownership back from a newer client.
func insertStateRow(ctx context.Context, session *vtgateconn.VTGateSession, name, keyspaceName, tableName, ownerToken string, vgtid *binlogdatapb.VGtid, tables map[string]*TableConfig, copyCompleted bool) error {
	latestVgtidJSON, tablesJSON, err := marshalState(vgtid, tables)
	if err != nil {
		return err
	}

	query := fmt.Sprintf(
		`insert into %s.%s (name, latest_vgtid, table_config, copy_completed, owner_token) values (:name, :latest_vgtid, :table_config, :copy_completed, :owner_token)`,
		keyspaceName, tableName,
	)
	_, err = session.Execute(ctx, query, map[string]*querypb.BindVariable{
		"name":           {Type: querypb.Type_VARBINARY, Value: []byte(name)},
		"latest_vgtid":   {Type: querypb.Type_JSON, Value: latestVgtidJSON},
		"table_config":   {Type: querypb.Type_JSON, Value: tablesJSON},
		"copy_completed": {Type: querypb.Type_INT64, Value: boolBindValue(copyCompleted)},
		"owner_token":    {Type: querypb.Type_VARBINARY, Value: []byte(ownerToken)},
	}, false)
	if err != nil {
		var sqlErr *sqlerror.SQLError
		if errors.As(sqlerror.NewSQLErrorFromError(err), &sqlErr) && sqlErr.Number() == sqlerror.ERDupEntry {
			return fmt.Errorf("%w: stream %s in %s.%s was initialized concurrently", ErrFenced, name, keyspaceName, tableName)
		}
		return fmt.Errorf("vstreamclient: failed to initialize state for %s.%s: %w", keyspaceName, tableName, err)
	}

	return nil
}

// updateStateRow replaces the stored position, table config, and copy flag for an existing state
// row. The write is predicated on this client's owner token, so a stale initializer that lost
// ownership after reading state cannot steal it back; it fails with ErrFenced instead.
func updateStateRow(ctx context.Context, session *vtgateconn.VTGateSession, name, keyspaceName, tableName, ownerToken string, vgtid *binlogdatapb.VGtid, tables map[string]*TableConfig, copyCompleted bool) error {
	latestVgtidJSON, tablesJSON, err := marshalState(vgtid, tables)
	if err != nil {
		return err
	}

	query := fmt.Sprintf(
		`update %s.%s set latest_vgtid = :latest_vgtid, table_config = :table_config, copy_completed = :copy_completed where name = :name and owner_token = :owner_token`,
		keyspaceName, tableName,
	)
	result, err := session.Execute(ctx, query, map[string]*querypb.BindVariable{
		"name":           {Type: querypb.Type_VARBINARY, Value: []byte(name)},
		"latest_vgtid":   {Type: querypb.Type_JSON, Value: latestVgtidJSON},
		"table_config":   {Type: querypb.Type_JSON, Value: tablesJSON},
		"copy_completed": {Type: querypb.Type_INT64, Value: boolBindValue(copyCompleted)},
		"owner_token":    {Type: querypb.Type_VARBINARY, Value: []byte(ownerToken)},
	}, false)
	if err != nil {
		return fmt.Errorf("vstreamclient: failed to initialize state for %s.%s: %w", keyspaceName, tableName, err)
	}

	if result.RowsAffected != 1 {
		return fmt.Errorf("%w: stream %s in %s.%s", ErrFenced, name, keyspaceName, tableName)
	}

	return nil
}

func boolBindValue(b bool) []byte {
	if b {
		return []byte("1")
	}
	return []byte("0")
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
			return nil, vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "vstreamclient: keyspace %s not found", table.Keyspace)
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

// claimStateOwnership stamps this client's owner token on an existing state row, fencing out any
// other client that is still running with the same stream name: its next checkpoint write will no
// longer match its own token and will fail with ErrFenced. This runs only after all validation has
// passed, so a constructor that fails can never leave the running client fenced.
func claimStateOwnership(ctx context.Context, session *vtgateconn.VTGateSession, name, keyspaceName, tableName, ownerToken string) error {
	query := fmt.Sprintf(`update %s.%s set owner_token = :owner_token where name = :name`, keyspaceName, tableName)

	result, err := session.Execute(ctx, query, map[string]*querypb.BindVariable{
		"owner_token": {Type: querypb.Type_VARBINARY, Value: []byte(ownerToken)},
		"name":        {Type: querypb.Type_VARBINARY, Value: []byte(name)},
	}, false)
	if err != nil {
		return fmt.Errorf("vstreamclient: failed to claim state ownership for %s.%s: %w", keyspaceName, tableName, err)
	}

	if result.RowsAffected != 1 {
		return fmt.Errorf("%w: stream %s in %s.%s disappeared while claiming ownership", ErrFenced, name, keyspaceName, tableName)
	}

	return nil
}

func getLatestVGtid(ctx context.Context, session *vtgateconn.VTGateSession, name, keyspaceName, tableName string) (vgtid *binlogdatapb.VGtid, tables map[string]dbTableConfig, copyCompleted, rowExists bool, err error) {
	query := fmt.Sprintf(`select latest_vgtid, table_config, copy_completed from %s.%s where name = :name`, keyspaceName, tableName)

	result, err := session.Execute(ctx, query, map[string]*querypb.BindVariable{
		"name": {Type: querypb.Type_VARBINARY, Value: []byte(name)},
	}, false)
	if err != nil {
		return nil, nil, false, false, fmt.Errorf("vstreamclient: failed to get latest vgtid for %s.%s: %w", keyspaceName, tableName, err)
	}

	if len(result.Rows) == 0 {
		return nil, nil, false, false, nil
	}

	// a row with a null vgtid is treated like missing state: the stream restarts from the
	// beginning, but through the update path since the row exists
	if result.Rows[0][0].IsNull() {
		return nil, nil, false, true, nil
	}

	// unmarshal the JSON value which should be a valid, initialized VGtid
	latestVGtidJSON, err := result.Rows[0][0].ToBytes()
	if err != nil {
		return nil, nil, false, true, fmt.Errorf("vstreamclient: failed to convert latest_vgtid to bytes: %w", err)
	}

	var latestVGtid binlogdatapb.VGtid
	err = protojson.Unmarshal(latestVGtidJSON, &latestVGtid)
	if err != nil {
		return nil, nil, false, true, fmt.Errorf("vstreamclient: failed to unmarshal latest_vgtid: %w", err)
	}

	// unmarshal the JSON value which should be the original table config. Unmarshalling into a
	// map of values (not pointers) means malformed entries like {"ks.t":null} become zero values
	// that fail table-config validation with a structured error instead of panicking.
	tablesJSON, err := result.Rows[0][1].ToBytes()
	if err != nil {
		return nil, nil, false, true, fmt.Errorf("vstreamclient: failed to convert table_config to bytes: %w", err)
	}

	err = json.Unmarshal(tablesJSON, &tables)
	if err != nil {
		return nil, nil, false, true, fmt.Errorf("vstreamclient: failed to unmarshal table_config: %w", err)
	}

	// check if the copy has been completed
	copyCompleted, err = result.Rows[0][2].ToBool()
	if err != nil {
		return nil, nil, false, true, fmt.Errorf("vstreamclient: failed to convert copy_completed to bool: %w", err)
	}

	return &latestVGtid, tables, copyCompleted, true, nil
}

func updateLatestVGtid(ctx context.Context, session *vtgateconn.VTGateSession, name, keyspaceName, tableName, ownerToken string, vgtid *binlogdatapb.VGtid, setCopyCompleted bool) error {
	latestVgtid, err := protojson.Marshal(vgtid)
	if err != nil {
		return fmt.Errorf("vstreamclient: failed to marshal latest_vgtid: %w", err)
	}

	// the owner_token predicate fences out this client once another client with the same stream
	// name has claimed ownership: the update then affects no rows and we fail with ErrFenced
	query := fmt.Sprintf(`update %s.%s set latest_vgtid = :latest_vgtid where name = :name and owner_token = :owner_token`, keyspaceName, tableName)
	if setCopyCompleted {
		query = fmt.Sprintf(`update %s.%s set latest_vgtid = :latest_vgtid, copy_completed = true where name = :name and owner_token = :owner_token`, keyspaceName, tableName)
	}
	result, err := session.Execute(ctx, query, map[string]*querypb.BindVariable{
		"latest_vgtid": {Type: querypb.Type_JSON, Value: latestVgtid},
		"name":         {Type: querypb.Type_VARBINARY, Value: []byte(name)},
		"owner_token":  {Type: querypb.Type_VARBINARY, Value: []byte(ownerToken)},
	}, false)
	if err != nil {
		return fmt.Errorf("vstreamclient: failed to update latest_vgtid for %s.%s: %w", keyspaceName, tableName, err)
	}

	if result.RowsAffected != 1 {
		return fmt.Errorf("%w: stream %s in %s.%s", ErrFenced, name, keyspaceName, tableName)
	}

	return nil
}
