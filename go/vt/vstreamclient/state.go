package vstreamclient

import (
	"context"
	"encoding/json"
	"fmt"

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

	fmt.Println("query: ", query)

	_, err := session.Execute(ctx, query, nil)
	if err != nil {
		return fmt.Errorf("vstreamclient: failed to create state table: %w", err)
	}

	return nil
}

func initVGtid(ctx context.Context, session *vtgateconn.VTGateSession, name, keyspaceName, tableName string, tables map[string]*TableConfig, shardsByKeyspace map[string][]string) (*binlogdatapb.VGtid, error) {
	vgtid, err := newVGtid(tables, shardsByKeyspace)
	if err != nil {
		return nil, err
	}

	latestVgtidJSON, err := json.Marshal(vgtid)
	if err != nil {
		return nil, fmt.Errorf("vstreamclient: failed to marshal latest vgtid: %w", err)
	}

	tablesJSON, err := json.Marshal(tablesToDBTableConfig(tables))
	if err != nil {
		return nil, fmt.Errorf("vstreamclient: failed to marshal tables: %w", err)
	}

	query := fmt.Sprintf(`insert into %s.%s (name, latest_vgtid, table_config) values (:name, :latest_vgtid, :table_config)`,
		keyspaceName, tableName,
	)
	_, err = session.Execute(ctx, query, map[string]*querypb.BindVariable{
		"name":         {Type: querypb.Type_VARBINARY, Value: []byte(name)},
		"latest_vgtid": {Type: querypb.Type_JSON, Value: latestVgtidJSON},
		"table_config": {Type: querypb.Type_JSON, Value: tablesJSON},
	})
	if err != nil {
		return nil, fmt.Errorf("vstreamclient: failed to get latest vgtid for %s.%s: %w", keyspaceName, tableName, err)
	}

	return vgtid, nil
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
	}

	return vgtid, nil
}

func getLatestVGtid(ctx context.Context, session *vtgateconn.VTGateSession, name, keyspaceName, tableName string) (*binlogdatapb.VGtid, map[string]*TableConfig, bool, error) {
	query := fmt.Sprintf(`select latest_vgtid, table_config, copy_completed from %s.%s where name = :name`, keyspaceName, tableName)

	result, err := session.Execute(ctx, query, map[string]*querypb.BindVariable{
		"name": {Type: querypb.Type_VARBINARY, Value: []byte(name)},
	})
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
	err = json.Unmarshal(latestVGtidJSON, &latestVGtid)
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

func updateLatestVGtid(ctx context.Context, session *vtgateconn.VTGateSession, name, keyspaceName, tableName string, vgtid *binlogdatapb.VGtid) error {
	latestVgtid, err := json.Marshal(vgtid)
	if err != nil {
		return fmt.Errorf("vstreamclient: failed to marshal latest_vgtid: %w", err)
	}

	query := fmt.Sprintf(`update %s.%s set latest_vgtid = :latest_vgtid where name = :name`,
		keyspaceName, tableName,
	)
	_, err = session.Execute(ctx, query, map[string]*querypb.BindVariable{
		"latest_vgtid": {Type: querypb.Type_JSON, Value: latestVgtid},
		"name":         {Type: querypb.Type_VARBINARY, Value: []byte(name)},
	})
	if err != nil {
		return fmt.Errorf("vstreamclient: failed to update latest_vgtid for %s.%s: %w", keyspaceName, tableName, err)
	}

	return nil
}

func setCopyCompleted(ctx context.Context, session *vtgateconn.VTGateSession, name, keyspaceName, tableName string) error {
	query := fmt.Sprintf(`update %s.%s set copy_completed = true where name = :name`,
		keyspaceName, tableName,
	)
	_, err := session.Execute(ctx, query, map[string]*querypb.BindVariable{
		"name": {Type: querypb.Type_VARBINARY, Value: []byte(name)},
	})
	if err != nil {
		return fmt.Errorf("vstreamclient: failed to set copy_completed for %s.%s: %w", keyspaceName, tableName, err)
	}

	return nil
}
