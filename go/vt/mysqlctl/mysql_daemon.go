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

package mysqlctl

import (
	"context"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/dbconnpool"
	"vitess.io/vitess/go/vt/mysqlctl/tmutils"

	querypb "vitess.io/vitess/go/vt/proto/query"
	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
)

// MysqlDaemon is the interface we use for abstracting Mysqld.
type MysqlDaemon interface {
	// methods related to mysql running or not
	Start(ctx context.Context, cnf *Mycnf, mysqldArgs ...string) error
	Shutdown(ctx context.Context, cnf *Mycnf, waitForMysqld bool) error
	RunMysqlUpgrade() error
	ReinitConfig(ctx context.Context, cnf *Mycnf) error
	Wait(ctx context.Context, cnf *Mycnf) error

	// GetMysqlPort returns the current port mysql is listening on.
	GetMysqlPort() (int32, error)

	// GetServerID returns the servers ID.
	GetServerID(ctx context.Context) (uint32, error)

	// GetServerUUID returns the servers UUID
	GetServerUUID(ctx context.Context) (string, error)

	// replication related methods
	StartReplication(hookExtraEnv map[string]string) error
	RestartReplication(hookExtraEnv map[string]string) error
	StartReplicationUntilAfter(ctx context.Context, pos mysql.Position) error
	StopReplication(hookExtraEnv map[string]string) error
	StopIOThread(ctx context.Context) error
	ReplicationStatus() (mysql.ReplicationStatus, error)
	PrimaryStatus(ctx context.Context) (mysql.PrimaryStatus, error)
	GetGTIDPurged(ctx context.Context) (mysql.Position, error)
	SetSemiSyncEnabled(source, replica bool) error
	SemiSyncEnabled() (source, replica bool)
	SemiSyncStatus() (source, replica bool)
	SemiSyncClients() (count uint32)
	SemiSyncSettings() (timeout uint64, numReplicas uint32)
	SemiSyncReplicationStatus() (bool, error)
	ResetReplicationParameters(ctx context.Context) error
	GetBinlogInformation(ctx context.Context) (binlogFormat string, logEnabled bool, logReplicaUpdate bool, binlogRowImage string, err error)
	GetGTIDMode(ctx context.Context) (gtidMode string, err error)

	// reparenting related methods
	ResetReplication(ctx context.Context) error
	PrimaryPosition() (mysql.Position, error)
	IsReadOnly() (bool, error)
	SetReadOnly(on bool) error
	SetSuperReadOnly(on bool) error
	SetReplicationPosition(ctx context.Context, pos mysql.Position) error
	SetReplicationSource(ctx context.Context, host string, port int, stopReplicationBefore bool, startReplicationAfter bool) error
	WaitForReparentJournal(ctx context.Context, timeCreatedNS int64) error

	WaitSourcePos(context.Context, mysql.Position) error

	// Promote makes the current server the primary. It will not change
	// the read_only state of the server.
	Promote(map[string]string) (mysql.Position, error)

	// Schema related methods
	GetSchema(ctx context.Context, dbName string, request *tabletmanagerdatapb.GetSchemaRequest) (*tabletmanagerdatapb.SchemaDefinition, error)
	GetColumns(ctx context.Context, dbName, table string) ([]*querypb.Field, []string, error)
	GetPrimaryKeyColumns(ctx context.Context, dbName, table string) ([]string, error)
	GetPrimaryKeyEquivalentColumns(ctx context.Context, dbName, table string) ([]string, error)
	PreflightSchemaChange(ctx context.Context, dbName string, changes []string) ([]*tabletmanagerdatapb.SchemaChangeResult, error)
	ApplySchemaChange(ctx context.Context, dbName string, change *tmutils.SchemaChange) (*tabletmanagerdatapb.SchemaChangeResult, error)

	// GetAppConnection returns a app connection to be able to talk to the database.
	GetAppConnection(ctx context.Context) (*dbconnpool.PooledDBConnection, error)
	// GetDbaConnection returns a dba connection.
	GetDbaConnection(ctx context.Context) (*dbconnpool.DBConnection, error)
	// GetAllPrivsConnection returns an allprivs connection (for user with all privileges except SUPER).
	GetAllPrivsConnection(ctx context.Context) (*dbconnpool.DBConnection, error)

	// GetVersionString returns the database version as a string
	GetVersionString() string

	// GetVersionComment returns the version comment
	GetVersionComment(ctx context.Context) string

	// ExecuteSuperQueryList executes a list of queries, no result
	ExecuteSuperQueryList(ctx context.Context, queryList []string) error

	// FetchSuperQuery executes one query, returns the result
	FetchSuperQuery(ctx context.Context, query string) (*sqltypes.Result, error)

	// EnableBinlogPlayback enables playback of binlog events
	EnableBinlogPlayback() error

	// DisableBinlogPlayback disable playback of binlog events
	DisableBinlogPlayback() error

	// Close will close this instance of Mysqld. It will wait for all dba
	// queries to be finished.
	Close()
}
