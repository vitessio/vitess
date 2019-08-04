/*
Copyright 2017 GitHub Inc.

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

package vttest

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"strings"
	"unicode"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/log"

	vschemapb "vitess.io/vitess/go/vt/proto/vschema"
	vttestpb "vitess.io/vitess/go/vt/proto/vttest"
)

// Config are the settings used to configure the self-contained Vitess cluster.
// The LocalCluster struct embeds Config so it's possible to either initialize
// a LocalCluster with the given settings, or set the settings directly after
// initialization.
// All settings must be set before LocalCluster.Setup() is called.
type Config struct {
	// Topology defines the fake cluster's topology. This field is mandatory.
	// See: vt/proto/vttest.VTTestTopology
	Topology *vttestpb.VTTestTopology

	// Seed can be set with a SeedConfig struct to enable
	// auto-initialization of the database cluster with random data.
	// If nil, no random initialization will be performed.
	// See: SeedConfig
	Seed *SeedConfig

	// SchemaDir is the directory for schema files. Within this dir,
	// there should be a subdir for each keyspace. Within each keyspace
	// dir, each file is executed as SQL after the database is created on
	// each shard.
	// If the directory contains a `vschema.json`` file, it will be used
	// as the VSchema for the V3 API
	SchemaDir string

	// DefaultSchemaDir is the default directory for initial schema files.
	// If no schema is found in SchemaDir, default to this location.
	DefaultSchemaDir string

	// DataDir is the directory where the data files will be placed.
	// If no directory is specified a random directory will be used
	// under VTDATAROOT.
	DataDir string

	// Charset is the default charset used by MySQL
	Charset string

	// WebDir is the location of the vtcld web server files
	WebDir string

	// WebDir2 is the location of the vtcld2 web server files
	WebDir2 string

	// ExtraMyCnf are the extra .CNF files to be added to the MySQL config
	ExtraMyCnf []string

	// OnlyMySQL can be set so only MySQL is initialized as part of the
	// local cluster configuration. The rest of the Vitess components will
	// not be started.
	OnlyMySQL bool

	// MySQL protocol bind address.
	// vtcombo will bind to this address when exposing the mysql protocol socket
	MySQLBindHost string

	// SnapshotFile is the path to the MySQL Snapshot that will be used to
	// initialize the mysqld instance in the cluster. Note that some environments
	// do not suppport initialization through snapshot files.
	SnapshotFile string

	// TransactionMode is SINGLE, MULTI or TWOPC
	TransactionMode string

	TransactionTimeout float64

	// The host name to use for the table otherwise it will be resolved from the local hostname
	TabletHostName string
}

// InitSchemas is a shortcut for tests that just want to setup a single
// keyspace with a single SQL file, and/or a vschema.
// It creates a temporary directory, and puts the schema/vschema in there.
// It then sets the right value for cfg.SchemaDir.
// At the end of the test, the caller should os.RemoveAll(cfg.SchemaDir).
func (cfg *Config) InitSchemas(keyspace, schema string, vschema *vschemapb.Keyspace) error {
	if cfg.SchemaDir != "" {
		return fmt.Errorf("SchemaDir is already set to %v", cfg.SchemaDir)
	}

	// Create a base temporary directory.
	tempSchemaDir, err := ioutil.TempDir("", "vttest")
	if err != nil {
		return err
	}

	// Write the schema if set.
	if schema != "" {
		ksDir := path.Join(tempSchemaDir, keyspace)
		err = os.Mkdir(ksDir, os.ModeDir|0775)
		if err != nil {
			return err
		}
		fileName := path.Join(ksDir, "schema.sql")
		err = ioutil.WriteFile(fileName, []byte(schema), 0666)
		if err != nil {
			return err
		}
	}

	// Write in the vschema if set.
	if vschema != nil {
		vschemaFilePath := path.Join(tempSchemaDir, keyspace, "vschema.json")
		vschemaJSON, err := json.Marshal(vschema)
		if err != nil {
			return err
		}
		if err := ioutil.WriteFile(vschemaFilePath, vschemaJSON, 0644); err != nil {
			return err
		}
	}
	cfg.SchemaDir = tempSchemaDir
	return nil
}

// DbName returns the default name for a database in this cluster.
// If OnlyMySQL is set, this will be the name of the single database
// created in MySQL. Otherwise, this will be blank.
func (cfg *Config) DbName() string {
	ns := cfg.Topology.GetKeyspaces()
	if len(ns) > 0 && cfg.OnlyMySQL {
		return ns[0].Name
	}
	return ""
}

// LocalCluster controls a local Vitess setup for testing, containing
// a MySQL instance and one or more vtgate-equivalent access points.
// To use, simply create a new LocalCluster instance and either pass in
// the desired Config, or manually set each field on the struct itself.
// Once the struct is configured, call LocalCluster.Setup() to instantiate
// the cluster.
// See: Config for configuration settings on the cluster
type LocalCluster struct {
	Config

	// Env is the Environment which will be used for unning this local cluster.
	// It can be set by the user before calling Setup(). If not set, Setup() will
	// use the NewDefaultEnv callback to instantiate an environment with the system
	// default settings
	Env Environment

	mysql MySQLManager
	vt    *VtProcess
}

// MySQLConnParams returns a mysql.ConnParams struct that can be used
// to connect directly to the mysqld service in the self-contained cluster
// This connection should be used for debug/introspection purposes; normal
// cluster access should be performed through the vtgate port.
func (db *LocalCluster) MySQLConnParams() mysql.ConnParams {
	return db.mysql.Params(db.DbName())
}

// MySQLAppDebugConnParams returns a mysql.ConnParams struct that can be used
// to connect directly to the mysqld service in the self-contained cluster,
// using the appdebug user. It's valid only if you used MySQLOnly option.
func (db *LocalCluster) MySQLAppDebugConnParams() mysql.ConnParams {
	connParams := db.MySQLConnParams()
	connParams.Uname = "vt_appdebug"
	return connParams
}

// Setup brings up the self-contained Vitess cluster by spinning up
// MySQL and Vitess instances. The spawned processes will be running
// until the TearDown() method is called.
// Please ensure to `defer db.TearDown()` after calling this method
func (db *LocalCluster) Setup() error {
	var err error

	if db.Env == nil {
		log.Info("No environment in cluster settings. Creating default...")
		db.Env, err = NewDefaultEnv()
		if err != nil {
			return err
		}
	}

	log.Infof("LocalCluster environment: %+v", db.Env)

	db.mysql, err = db.Env.MySQLManager(db.ExtraMyCnf, db.SnapshotFile)
	if err != nil {
		return err
	}

	log.Infof("Initializing MySQL Manager (%T)...", db.mysql)

	if err := db.mysql.Setup(); err != nil {
		log.Errorf("Mysqlctl failed to start: %s", err)
		if err, ok := err.(*exec.ExitError); ok {
			log.Errorf("stderr: %s", err.Stderr)
		}
		return err
	}

	mycfg, _ := json.Marshal(db.mysql.Params(""))
	log.Infof("MySQL up: %s", mycfg)

	if err := db.createDatabases(); err != nil {
		return err
	}

	if err := db.loadSchema(); err != nil {
		return err
	}

	if db.Seed != nil {
		if err := db.populateWithRandomData(); err != nil {
			return err
		}
	}

	if !db.OnlyMySQL {
		log.Infof("Starting vtcombo...")
		db.vt = VtcomboProcess(db.Env, &db.Config, db.mysql)
		if err := db.vt.WaitStart(); err != nil {
			return err
		}
		log.Infof("vtcombo up: %s", db.vt.Address())
	}

	return nil
}

// TearDown shuts down all the processes in the local cluster
// and cleans up any temporary on-disk data.
// If an error is returned, some of the running processes may not
// have been shut down cleanly and may need manual cleanup.
func (db *LocalCluster) TearDown() error {
	var errors []string

	if db.vt != nil {
		if err := db.vt.WaitTerminate(); err != nil {
			errors = append(errors, fmt.Sprintf("vtprocess: %s", err))
		}
	}

	if err := db.mysql.TearDown(); err != nil {
		errors = append(errors, fmt.Sprintf("mysql: %s", err))

		log.Errorf("failed to shutdown MySQL: %s", err)
		if err, ok := err.(*exec.ExitError); ok {
			log.Errorf("stderr: %s", err.Stderr)
		}
	}

	if err := db.Env.TearDown(); err != nil {
		errors = append(errors, fmt.Sprintf("environment: %s", err))
	}

	if len(errors) > 0 {
		return fmt.Errorf("failed to teardown LocalCluster:\n%s",
			strings.Join(errors, "\n"))
	}

	return nil
}

func (db *LocalCluster) shardNames(keyspace *vttestpb.Keyspace) (names []string) {
	for _, spb := range keyspace.Shards {
		dbname := spb.DbNameOverride
		if dbname == "" {
			dbname = fmt.Sprintf("vt_%s_%s", keyspace.Name, spb.Name)
		}
		names = append(names, dbname)
	}
	return
}

func isDir(path string) bool {
	info, err := os.Stat(path)
	return err == nil && info.IsDir()
}

func (db *LocalCluster) loadSchema() error {
	if db.SchemaDir == "" {
		return nil
	}

	log.Info("Loading custom schema...")

	if !isDir(db.SchemaDir) {
		return fmt.Errorf("LoadSchema(): SchemaDir does not exist")
	}

	for _, kpb := range db.Topology.Keyspaces {
		if kpb.ServedFrom != "" {
			// redirected keyspaces have no underlying database
			continue
		}

		keyspace := kpb.Name
		keyspaceDir := path.Join(db.SchemaDir, keyspace)

		schemaDir := keyspaceDir
		if !isDir(schemaDir) {
			schemaDir = db.DefaultSchemaDir
			if schemaDir == "" || !isDir(schemaDir) {
				return fmt.Errorf("LoadSchema: schema dir for ks `%s` does not exist (%s)", keyspace, schemaDir)
			}
		}

		glob, _ := filepath.Glob(path.Join(schemaDir, "*.sql"))
		for _, filepath := range glob {
			cmds, err := LoadSQLFile(filepath, schemaDir)
			if err != nil {
				return err
			}

			for _, dbname := range db.shardNames(kpb) {
				if err := db.Execute(cmds, dbname); err != nil {
					return err
				}
			}
		}
	}

	return nil
}

func (db *LocalCluster) createDatabases() error {
	log.Info("Creating databases in cluster...")

	var sql []string
	for _, kpb := range db.Topology.Keyspaces {
		if kpb.ServedFrom != "" {
			continue
		}
		for _, dbname := range db.shardNames(kpb) {
			sql = append(sql, fmt.Sprintf("create database `%s`", dbname))
		}
	}
	return db.Execute(sql, "")
}

// Execute runs a series of SQL statements on the MySQL instance backing
// this local cluster. This is provided for debug/introspection purposes;
// normal cluster access should be performed through the Vitess GRPC interface.
func (db *LocalCluster) Execute(sql []string, dbname string) error {
	params := db.mysql.Params(dbname)
	conn, err := mysql.Connect(context.Background(), &params)
	if err != nil {
		return err
	}
	defer conn.Close()

	_, err = conn.ExecuteFetch("START TRANSACTION", 0, false)
	if err != nil {
		return err
	}

	for _, cmd := range sql {
		log.Infof("Execute(%s): \"%s\"", dbname, cmd)
		_, err := conn.ExecuteFetch(cmd, 0, false)
		if err != nil {
			return err
		}
	}

	_, err = conn.ExecuteFetch("COMMIT", 0, false)
	return err
}

// Query runs a  SQL query on the MySQL instance backing this local cluster and returns
// its result. This is provided for debug/introspection purposes;
// normal cluster access should be performed through the Vitess GRPC interface.
func (db *LocalCluster) Query(sql, dbname string, limit int) (*sqltypes.Result, error) {
	params := db.mysql.Params(dbname)
	conn, err := mysql.Connect(context.Background(), &params)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	return conn.ExecuteFetch(sql, limit, false)
}

// JSONConfig returns a key/value object with the configuration
// settings for the local cluster. It should be serialized with
// `json.Marshal`
func (db *LocalCluster) JSONConfig() interface{} {
	if db.OnlyMySQL {
		return db.mysql.Params("")
	}

	config := map[string]interface{}{
		"port":               db.vt.Port,
		"socket":             db.mysql.UnixSocket(),
		"vtcombo_mysql_port": db.Env.PortForProtocol("vtcombo_mysql_port", ""),
		"mysql":              db.Env.PortForProtocol("mysql", ""),
	}

	if grpc := db.vt.PortGrpc; grpc != 0 {
		config["grpc_port"] = grpc
	}

	return config
}

// LoadSQLFile loads a parses a .sql file from disk, removing all the
// different comments that mysql/mysqldump inserts in these, and returning
// each individual SQL statement as its own string.
// If sourceroot is set, that directory will be used when resolving `source `
// statements in the SQL file.
func LoadSQLFile(filename, sourceroot string) ([]string, error) {
	var (
		cmd  bytes.Buffer
		sql  []string
		inSQ bool
		inDQ bool
	)

	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		line = strings.TrimRightFunc(line, unicode.IsSpace)

		if !inSQ && !inDQ && strings.HasPrefix(line, "--") {
			continue
		}

		var i, next int
		for {
			i = next
			if i >= len(line) {
				break
			}

			next = i + 1

			if line[i] == '\\' {
				next = i + 2
			} else if line[i] == '\'' && !inDQ {
				inSQ = !inSQ
			} else if line[i] == '"' && !inSQ {
				inDQ = !inDQ
			} else if !inSQ && !inDQ {
				if line[i] == '#' || strings.HasPrefix(line[i:], "-- ") {
					line = line[:i]
					break
				}
				if line[i] == ';' {
					cmd.WriteString(line[:i])
					sql = append(sql, cmd.String())
					cmd.Reset()

					line = line[i+1:]
					next = 0
				}
			}
		}

		if strings.TrimSpace(line) != "" {
			if sourceroot != "" && cmd.Len() == 0 && strings.HasPrefix(line, "source ") {
				srcfile := path.Join(sourceroot, line[7:])
				sql2, err := LoadSQLFile(srcfile, sourceroot)
				if err != nil {
					return nil, err
				}
				sql = append(sql, sql2...)
			} else {
				cmd.WriteString(line)
				cmd.WriteByte('\n')
			}
		}
	}

	if cmd.Len() != 0 {
		sql = append(sql, cmd.String())
	}

	if err := scanner.Err(); err != nil {
		return nil, err
	}

	return sql, nil
}
