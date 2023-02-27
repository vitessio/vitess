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
package vtgate

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	"vitess.io/vitess/go/vt/proto/topodata"
)

func Test_suggestTabletType_to_replica(t *testing.T) {
	type args struct {
		readWriteSplittingPolicy string
		inTransaction            bool
		hasCreatedTempTables     bool
		hasAdvisoryLock          bool
		sql                      string
	}
	tests := []struct {
		name           string
		args           args
		wantTabletType topodata.TabletType
		wantErr        assert.ErrorAssertionFunc
	}{
		{
			name: "readWriteSplittingPolicy=enable, inTransaction=false, hasCreatedTempTables=false, hasAdvisoryLock=false",
			args: args{
				readWriteSplittingPolicy: "enable",
				inTransaction:            false,
				hasCreatedTempTables:     false,
				hasAdvisoryLock:          false,
				sql:                      "SELECT * FROM users;",
			},
			wantTabletType: topodata.TabletType_REPLICA,
			wantErr:        assert.NoError,
		},
		{
			name: "readWriteSplittingPolicy=enable, inTransaction=false, hasCreatedTempTables=false, hasAdvisoryLock=false",
			args: args{
				readWriteSplittingPolicy: "enable",
				inTransaction:            false,
				hasCreatedTempTables:     false,
				hasAdvisoryLock:          false,
				sql:                      "select * from users union all select * from users;",
			},
			wantTabletType: topodata.TabletType_REPLICA,
			wantErr:        assert.NoError,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotTabletType, err := suggestTabletType(tt.args.readWriteSplittingPolicy, tt.args.inTransaction, tt.args.hasCreatedTempTables, tt.args.hasAdvisoryLock, tt.args.sql)
			if !tt.wantErr(t, err, fmt.Sprintf("suggestTabletType(%v, %v, %v, %v, %v)", tt.args.readWriteSplittingPolicy, tt.args.inTransaction, tt.args.hasCreatedTempTables, tt.args.hasAdvisoryLock, tt.args.sql)) {
				return
			}
			assert.Equalf(t, tt.wantTabletType, gotTabletType, "suggestTabletType(%v, %v, %v, %v, %v)", tt.args.readWriteSplittingPolicy, tt.args.inTransaction, tt.args.hasCreatedTempTables, tt.args.hasAdvisoryLock, tt.args.sql)
		})
	}
}

func Test_suggestTabletType_to_primary(t *testing.T) {
	type args struct {
		readWriteSplittingPolicy string
		inTransaction            bool
		hasCreatedTempTables     bool
		hasAdvisoryLock          bool
		sql                      string
	}
	tests := []struct {
		name           string
		args           args
		wantTabletType topodata.TabletType
		wantErr        assert.ErrorAssertionFunc
	}{
		{
			name: "readWriteSplittingPolicy=disable, inTransaction=false, hasCreatedTempTables=false, hasAdvisoryLock=false",
			args: args{
				readWriteSplittingPolicy: "disable",
				inTransaction:            false,
				hasCreatedTempTables:     false,
				hasAdvisoryLock:          false,
				sql:                      "SELECT * FROM users;",
			},
			wantTabletType: topodata.TabletType_PRIMARY,
			wantErr:        assert.NoError,
		},
		{
			name: "readWriteSplittingPolicy=disable, inTransaction=false, hasCreatedTempTables=false, hasAdvisoryLock=false",
			args: args{
				readWriteSplittingPolicy: "disable",
				inTransaction:            false,
				hasCreatedTempTables:     false,
				hasAdvisoryLock:          false,
				sql:                      "select * from users union all select * from users;",
			},
			wantTabletType: topodata.TabletType_PRIMARY,
			wantErr:        assert.NoError,
		},
		{
			name: "readWriteSplittingPolicy=enable, inTransaction=false, hasCreatedTempTables=false, hasAdvisoryLock=false",
			args: args{
				readWriteSplittingPolicy: "enable",
				inTransaction:            false,
				hasCreatedTempTables:     false,
				hasAdvisoryLock:          false,
				sql:                      "INSERT INTO users (id, name) VALUES (1, 'foo');",
			},
			wantTabletType: topodata.TabletType_PRIMARY,
			wantErr:        assert.NoError,
		},
		{
			name: "readWriteSplittingPolicy=enable, inTransaction=false, hasCreatedTempTables=false, hasAdvisoryLock=false",
			args: args{
				readWriteSplittingPolicy: "enable",
				inTransaction:            false,
				hasCreatedTempTables:     false,
				hasAdvisoryLock:          false,
				sql:                      "UPDATE users SET name = 'foo' WHERE id = 1;",
			},
			wantTabletType: topodata.TabletType_PRIMARY,
			wantErr:        assert.NoError,
		},
		{
			name: "readWriteSplittingPolicy=enable, inTransaction=false, hasCreatedTempTables=false, hasAdvisoryLock=false",
			args: args{
				readWriteSplittingPolicy: "enable",
				inTransaction:            false,
				hasCreatedTempTables:     false,
				hasAdvisoryLock:          false,
				sql:                      "DELETE FROM users WHERE id = 1;",
			},
			wantTabletType: topodata.TabletType_PRIMARY,
			wantErr:        assert.NoError,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotTabletType, err := suggestTabletType(tt.args.readWriteSplittingPolicy, tt.args.inTransaction, tt.args.hasCreatedTempTables, tt.args.hasAdvisoryLock, tt.args.sql)
			if !tt.wantErr(t, err, fmt.Sprintf("suggestTabletType(%v, %v, %v, %v, %v)", tt.args.readWriteSplittingPolicy, tt.args.inTransaction, tt.args.hasCreatedTempTables, tt.args.hasAdvisoryLock, tt.args.sql)) {
				return
			}
			assert.Equalf(t, tt.wantTabletType, gotTabletType, "suggestTabletType(%v, %v, %v, %v, %v)", tt.args.readWriteSplittingPolicy, tt.args.inTransaction, tt.args.hasCreatedTempTables, tt.args.hasAdvisoryLock, tt.args.sql)
		})
	}
}

func Test_suggestTabletType_force_primary(t *testing.T) {
	type args struct {
		readWriteSplittingPolicy string
		inTransaction            bool
		hasCreatedTempTables     bool
		hasAdvisoryLock          bool
		sql                      string
	}
	tests := []struct {
		name           string
		args           args
		wantTabletType topodata.TabletType
		wantErr        assert.ErrorAssertionFunc
	}{
		{
			name: "readWriteSplittingPolicy=enable, inTransaction=true, hasCreatedTempTables=false, hasAdvisoryLock=false",
			args: args{
				readWriteSplittingPolicy: "enable",
				inTransaction:            true,
				hasCreatedTempTables:     false,
				hasAdvisoryLock:          false,
				sql:                      "SELECT * FROM users;",
			},
			wantTabletType: topodata.TabletType_PRIMARY,
			wantErr:        assert.NoError,
		},
		{
			name: "readWriteSplittingPolicy=enable, inTransaction=false, hasCreatedTempTables=true, hasAdvisoryLock=false",
			args: args{
				readWriteSplittingPolicy: "enable",
				inTransaction:            false,
				hasCreatedTempTables:     true,
				hasAdvisoryLock:          false,
				sql:                      "SELECT * FROM users;",
			},
			wantTabletType: topodata.TabletType_PRIMARY,
			wantErr:        assert.NoError,
		},
		{
			name: "readWriteSplittingPolicy=enable, inTransaction=false, hasCreatedTempTables=false, hasAdvisoryLock=true",
			args: args{
				readWriteSplittingPolicy: "enable",
				inTransaction:            false,
				hasCreatedTempTables:     false,
				hasAdvisoryLock:          true,
				sql:                      "SELECT * FROM users;",
			},
			wantTabletType: topodata.TabletType_PRIMARY,
			wantErr:        assert.NoError,
		},
		{
			name: "readWriteSplittingPolicy=enable, inTransaction=false, hasCreatedTempTables=false, hasAdvisoryLock=false",
			args: args{
				readWriteSplittingPolicy: "enable",
				inTransaction:            false,
				hasCreatedTempTables:     false,
				hasAdvisoryLock:          false,
				sql:                      "SELECT last_insert_id();",
			},
			wantTabletType: topodata.TabletType_PRIMARY,
			wantErr:        assert.NoError,
		},
		{
			name: "readWriteSplittingPolicy=enable, inTransaction=false, hasCreatedTempTables=false, hasAdvisoryLock=false",
			args: args{
				readWriteSplittingPolicy: "enable",
				inTransaction:            false,
				hasCreatedTempTables:     false,
				hasAdvisoryLock:          false,
				sql:                      "SELECT * FROM users lock in share mode;",
			},
			wantTabletType: topodata.TabletType_PRIMARY,
			wantErr:        assert.NoError,
		},
		{
			name: "readWriteSplittingPolicy=enable, inTransaction=false, hasCreatedTempTables=false, hasAdvisoryLock=false",
			args: args{
				readWriteSplittingPolicy: "enable",
				inTransaction:            false,
				hasCreatedTempTables:     false,
				hasAdvisoryLock:          false,
				sql:                      "SELECT * FROM users for update;",
			},
			wantTabletType: topodata.TabletType_PRIMARY,
			wantErr:        assert.NoError,
		},
		{
			name: "readWriteSplittingPolicy=enable, inTransaction=false, hasCreatedTempTables=false, hasAdvisoryLock=false",
			args: args{
				readWriteSplittingPolicy: "enable",
				inTransaction:            false,
				hasCreatedTempTables:     false,
				hasAdvisoryLock:          false,
				sql:                      "SELECT get_lock('lock', 10);",
			},
			wantTabletType: topodata.TabletType_PRIMARY,
			wantErr:        assert.NoError,
		},
		{
			name: "readWriteSplittingPolicy=enable, inTransaction=false, hasCreatedTempTables=false, hasAdvisoryLock=false",
			args: args{
				readWriteSplittingPolicy: "enable",
				inTransaction:            false,
				hasCreatedTempTables:     false,
				hasAdvisoryLock:          false,
				sql:                      "SELECT release_lock('lock');",
			},
			wantTabletType: topodata.TabletType_PRIMARY,
			wantErr:        assert.NoError,
		},
		{
			name: "readWriteSplittingPolicy=enable, inTransaction=false, hasCreatedTempTables=false, hasAdvisoryLock=false",
			args: args{
				readWriteSplittingPolicy: "enable",
				inTransaction:            false,
				hasCreatedTempTables:     false,
				hasAdvisoryLock:          false,
				sql:                      "SELECT is_used_lock('lock');",
			},
			wantTabletType: topodata.TabletType_PRIMARY,
			wantErr:        assert.NoError,
		},
		{
			name: "readWriteSplittingPolicy=enable, inTransaction=false, hasCreatedTempTables=false, hasAdvisoryLock=false",
			args: args{
				readWriteSplittingPolicy: "enable",
				inTransaction:            false,
				hasCreatedTempTables:     false,
				hasAdvisoryLock:          false,
				sql:                      "SELECT is_free_lock('lock');",
			},
			wantTabletType: topodata.TabletType_PRIMARY,
			wantErr:        assert.NoError,
		},
		{
			name: "readWriteSplittingPolicy=enable, inTransaction=false, hasCreatedTempTables=false, hasAdvisoryLock=false",
			args: args{
				readWriteSplittingPolicy: "enable",
				inTransaction:            false,
				hasCreatedTempTables:     false,
				hasAdvisoryLock:          false,
				sql:                      "SELECT release_all_locks();",
			},
			wantTabletType: topodata.TabletType_PRIMARY,
			wantErr:        assert.NoError,
		},
		{
			name: "readWriteSplittingPolicy=enable, inTransaction=false, hasCreatedTempTables=false, hasAdvisoryLock=false",
			args: args{
				readWriteSplittingPolicy: "enable",
				inTransaction:            false,
				hasCreatedTempTables:     false,
				hasAdvisoryLock:          false,
				sql:                      "SELECT * from performance_schema.metadata_locks;",
			},
			wantTabletType: topodata.TabletType_PRIMARY,
			wantErr:        assert.NoError,
		},
		{
			name: "readWriteSplittingPolicy=enable, inTransaction=false, hasCreatedTempTables=false, hasAdvisoryLock=false",
			args: args{
				readWriteSplittingPolicy: "enable",
				inTransaction:            false,
				hasCreatedTempTables:     false,
				hasAdvisoryLock:          false,
				sql:                      "SELECT * from information_schema.innodb_trx;",
			},
			wantTabletType: topodata.TabletType_PRIMARY,
			wantErr:        assert.NoError,
		},
		{
			name: "readWriteSplittingPolicy=enable, inTransaction=false, hasCreatedTempTables=false, hasAdvisoryLock=false",
			args: args{
				readWriteSplittingPolicy: "enable",
				inTransaction:            false,
				hasCreatedTempTables:     false,
				hasAdvisoryLock:          false,
				sql:                      "SELECT * from mysql.user",
			},
			wantTabletType: topodata.TabletType_PRIMARY,
			wantErr:        assert.NoError,
		},
		{
			name: "readWriteSplittingPolicy=enable, inTransaction=false, hasCreatedTempTables=false, hasAdvisoryLock=false",
			args: args{
				readWriteSplittingPolicy: "enable",
				inTransaction:            false,
				hasCreatedTempTables:     false,
				hasAdvisoryLock:          false,
				sql:                      "SELECT * from sys.sys_config;",
			},
			wantTabletType: topodata.TabletType_PRIMARY,
			wantErr:        assert.NoError,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotTabletType, err := suggestTabletType(tt.args.readWriteSplittingPolicy, tt.args.inTransaction, tt.args.hasCreatedTempTables, tt.args.hasAdvisoryLock, tt.args.sql)
			if !tt.wantErr(t, err, fmt.Sprintf("suggestTabletType(%v, %v, %v, %v, %v)", tt.args.readWriteSplittingPolicy, tt.args.inTransaction, tt.args.hasCreatedTempTables, tt.args.hasAdvisoryLock, tt.args.sql)) {
				return
			}
			assert.Equalf(t, tt.wantTabletType, gotTabletType, "suggestTabletType(%v, %v, %v, %v, %v)", tt.args.readWriteSplittingPolicy, tt.args.inTransaction, tt.args.hasCreatedTempTables, tt.args.hasAdvisoryLock, tt.args.sql)
		})
	}
}
