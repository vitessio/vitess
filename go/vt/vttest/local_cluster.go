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

package vttest

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"strings"
	"time"
	"unicode"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/encoding/prototext"
	"google.golang.org/protobuf/proto"

	"vitess.io/vitess/go/vt/vtenv"

	"vitess.io/vitess/go/constants/sidecar"
	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/proto/logutil"
	"vitess.io/vitess/go/vt/sidecardb"
	"vitess.io/vitess/go/vt/vtctl/vtctlclient"

	vschemapb "vitess.io/vitess/go/vt/proto/vschema"
	vttestpb "vitess.io/vitess/go/vt/proto/vttest"

	// we need to import the grpcvtctlclient library so the gRPC
	// vtctl client is registered and can be used.
	_ "vitess.io/vitess/go/vt/vtctl/grpcvtctlclient"
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

	// PlannerVersion is the planner version to use for the vtgate.
	// Choose between Gen4, Gen4Greedy and Gen4Left2Right
	PlannerVersion string

	// ExtraMyCnf are the extra .CNF files to be added to the MySQL config
	ExtraMyCnf []string

	// OnlyMySQL can be set so only MySQL is initialized as part of the
	// local cluster configuration. The rest of the Vitess components will
	// not be started.
	OnlyMySQL bool

	// PersistentMode can be set so that MySQL data directory is not cleaned up
	// when LocalCluster.TearDown() is called. This is useful for running
	// vttestserver as a database container in local developer environments. Note
	// that db and vschema migration files (-schema_dir option) and seeding of
	// random data (-initialize_with_random_data option) will only run during
	// cluster startup if the data directory does not already exist.
	PersistentMode bool

	// VtCombo bind address.
	// vtcombo will bind to this address when running the servenv.
	VtComboBindAddress string

	// MySQL protocol bind address.
	// vtcombo will bind to this address when exposing the mysql protocol socket
	MySQLBindHost string
	// SnapshotFile is the path to the MySQL Snapshot that will be used to
	// initialize the mysqld instance in the cluster. Note that some environments
	// do not suppport initialization through snapshot files.
	SnapshotFile string

	// Enable system settings to be changed per session at the database connection level
	EnableSystemSettings bool

	// TransactionMode is SINGLE, MULTI or TWOPC
	TransactionMode string

	TransactionTimeout float64

	// The host name to use for the table otherwise it will be resolved from the local hostname
	TabletHostName string

	// Whether to enable/disable workflow manager
	InitWorkflowManager bool

	// Authorize vschema ddl operations to a list of users
	VSchemaDDLAuthorizedUsers string

	// How to handle foreign key constraint in CREATE/ALTER TABLE.  Valid values are "allow", "disallow"
	ForeignKeyMode string

	// Allow users to submit, view, and control Online DDL
	EnableOnlineDDL bool

	// Allow users to submit direct DDL statements
	EnableDirectDDL bool

	// Allow users to start a local cluster using a remote topo server
	ExternalTopoImplementation string

	ExternalTopoGlobalServerAddress string

	ExternalTopoGlobalRoot string

	VtgateTabletRefreshInterval time.Duration
}

// InitSchemas is a shortcut for tests that just want to setup a single
// keyspace with a single SQL file, and/or a vschema.
// It creates a temporary directory, and puts the schema/vschema in there.
// It then sets the right value for cfg.SchemaDir.
// At the end of the test, the caller should os.RemoveAll(cfg.SchemaDir).
func (cfg *Config) InitSchemas(keyspace, schema string, vschema *vschemapb.Keyspace) error {
	schemaDir := cfg.SchemaDir
	if schemaDir == "" {
		// Create a base temporary directory.
		tempSchemaDir, err := os.MkdirTemp("", "vttest")
		if err != nil {
			return err
		}
		schemaDir = tempSchemaDir
	}

	// Write the schema if set.
	if schema != "" {
		ksDir := path.Join(schemaDir, keyspace)
		err := os.Mkdir(ksDir, os.ModeDir|0o775)
		if err != nil {
			return err
		}
		fileName := path.Join(ksDir, "schema.sql")
		err = os.WriteFile(fileName, []byte(schema), 0o666)
		if err != nil {
			return err
		}
	}

	// Write in the vschema if set.
	if vschema != nil {
		vschemaFilePath := path.Join(schemaDir, keyspace, "vschema.json")
		vschemaJSON, err := json.Marshal(vschema)
		if err != nil {
			return err
		}
		if err := os.WriteFile(vschemaFilePath, vschemaJSON, 0o644); err != nil {
			return err
		}
	}
	cfg.SchemaDir = schemaDir
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

// TopoData is a struct representing a test topology.
//
// It implements pflag.Value and can be used as a destination command-line via
// pflag.Var or pflag.VarP.
type TopoData struct {
	vtTestTopology *vttestpb.VTTestTopology
	unmarshal      func(b []byte, m proto.Message) error
}

// String is part of the pflag.Value interface.
func (td *TopoData) String() string {
	return prototext.Format(td.vtTestTopology)
}

// Set is part of the pflag.Value interface.
func (td *TopoData) Set(value string) error {
	return td.unmarshal([]byte(value), td.vtTestTopology)
}

// Type is part of the pflag.Value interface.
func (td *TopoData) Type() string { return "vttest.TopoData" }

// TextTopoData returns a test TopoData that unmarshals using
// prototext.Unmarshal.
func TextTopoData(tpb *vttestpb.VTTestTopology) *TopoData {
	return &TopoData{
		vtTestTopology: tpb,
		unmarshal:      prototext.Unmarshal,
	}
}

// JSONTopoData returns a test TopoData that unmarshals using
// protojson.Unmarshal.
func JSONTopoData(tpb *vttestpb.VTTestTopology) *TopoData {
	return &TopoData{
		vtTestTopology: tpb,
		unmarshal:      protojson.Unmarshal,
	}
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
	topo  TopoManager
	vt    *VtProcess
}

// MySQLConnParams returns a mysql.ConnParams struct that can be used
// to connect directly to the mysqld service in the self-contained cluster
// This connection should be used for debug/introspection purposes; normal
// cluster access should be performed through the vtgate port.
func (db *LocalCluster) MySQLConnParams() mysql.ConnParams {
	connParams := db.mysql.Params(db.DbName())
	ch, err := collations.MySQL8().ParseConnectionCharset(db.Config.Charset)
	if err != nil {
		panic(err)
	}
	connParams.Charset = ch
	return connParams
}

// MySQLAppDebugConnParams returns a mysql.ConnParams struct that can be used
// to connect directly to the mysqld service in the self-contained cluster,
// using the appdebug user. It's valid only if you used MySQLOnly option.
func (db *LocalCluster) MySQLAppDebugConnParams() mysql.ConnParams {
	connParams := db.MySQLConnParams()
	connParams.Uname = "vt_appdebug"
	return connParams
}

// MySQLCleanConnParams returns connection params that can be used to connect
// directly to MySQL, even if there's a toxyproxy instance on the way.
func (db *LocalCluster) MySQLCleanConnParams() mysql.ConnParams {
	mysqlctl := db.mysql
	if toxiproxy, ok := mysqlctl.(*Toxiproxyctl); ok {
		mysqlctl = toxiproxy.mysqlctl
	}
	connParams := mysqlctl.Params(db.DbName())
	ch, err := collations.MySQL8().ParseConnectionCharset(db.Config.Charset)
	if err != nil {
		panic(err)
	}
	connParams.Charset = ch
	return connParams
}

// SimulateMySQLHang simulates a scenario where the backend MySQL stops all data from flowing through.
// Please ensure to `defer db.StopSimulateMySQLHang()` after calling this method.
func (db *LocalCluster) SimulateMySQLHang() error {
	if toxiproxy, ok := db.mysql.(*Toxiproxyctl); ok {
		return toxiproxy.AddTimeoutToxic()
	}
	return fmt.Errorf("cannot simulate MySQL hang on non-Toxiproxyctl MySQLManager %v", db.mysql)
}

// PauseSimulateMySQLHang pauses the MySQL hang simulation to allow queries to go through.
// This is useful when you want to allow new queries to go through, but keep the existing ones hanging.
func (db *LocalCluster) PauseSimulateMySQLHang() error {
	if toxiproxy, ok := db.mysql.(*Toxiproxyctl); ok {
		return toxiproxy.UpdateTimeoutToxicity(0)
	}
	return fmt.Errorf("cannot simulate MySQL hang on non-Toxiproxyctl MySQLManager %v", db.mysql)
}

// StopSimulateMySQLHang stops the MySQL hang simulation to allow queries to go through.
func (db *LocalCluster) StopSimulateMySQLHang() error {
	if toxiproxy, ok := db.mysql.(*Toxiproxyctl); ok {
		return toxiproxy.RemoveTimeoutToxic()
	}
	return fmt.Errorf("cannot simulate MySQL hang on non-Toxiproxyctl MySQLManager %v", db.mysql)
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

	// Set up topo manager if we are using a remote topo server
	if db.ExternalTopoImplementation != "" {
		db.topo = db.Env.TopoManager(db.ExternalTopoImplementation, db.ExternalTopoGlobalServerAddress, db.ExternalTopoGlobalRoot, db.Topology)
		log.Infof("Initializing Topo Manager: %+v", db.topo)
		if err := db.topo.Setup(); err != nil {
			log.Errorf("Failed to set up Topo Manager: %v", err)
			return err
		}
	}

	db.mysql, err = db.Env.MySQLManager(db.ExtraMyCnf, db.SnapshotFile)
	if err != nil {
		return err
	}

	initializing := true
	if db.PersistentMode && dirExist(db.mysql.TabletDir()) {
		initializing = false
	}

	if initializing {
		log.Infof("Initializing MySQL Manager (%T)...", db.mysql)
		if err := db.mysql.Setup(); err != nil {
			log.Errorf("Mysqlctl failed to start: %s", err)
			if err, ok := err.(*exec.ExitError); ok {
				log.Errorf("stderr: %s", err.Stderr)
			}
			return err
		}

		if err := db.createDatabases(); err != nil {
			return err
		}
	} else {
		log.Infof("Starting MySQL Manager (%T)...", db.mysql)
		if err := db.mysql.Start(); err != nil {
			log.Errorf("Mysqlctl failed to start: %s", err)
			if err, ok := err.(*exec.ExitError); ok {
				log.Errorf("stderr: %s", err.Stderr)
			}
			return err
		}
	}

	mycfg, _ := json.Marshal(db.mysql.Params(""))
	log.Infof("MySQL up: %s", mycfg)

	if !db.OnlyMySQL {
		log.Infof("Starting vtcombo...")
		db.vt, _ = VtcomboProcess(db.Env, &db.Config, db.mysql)
		if err := db.vt.WaitStart(); err != nil {
			return err
		}
		log.Infof("vtcombo up: %s", db.vt.Address())
	}

	if initializing {
		log.Info("Mysql data directory does not exist. Initializing cluster with database and vschema migrations...")
		// Load schema will apply db and vschema migrations. Running after vtcombo starts to be able to apply vschema migrations
		if err := db.loadSchema(true); err != nil {
			return err
		}

		if db.Seed != nil {
			log.Info("Populating database with random data...")
			if err := db.populateWithRandomData(); err != nil {
				return err
			}
		}
	} else {
		log.Info("Mysql data directory exists in persistent mode. Will only execute vschema migrations during startup")
		if err := db.loadSchema(false); err != nil {
			return err
		}
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

	if !db.PersistentMode {
		if err := db.Env.TearDown(); err != nil {
			errors = append(errors, fmt.Sprintf("environment: %s", err))
		}
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

// loadSchema applies sql and vschema migrations respectively for each keyspace in the topology
func (db *LocalCluster) loadSchema(shouldRunDatabaseMigrations bool) error {
	if db.SchemaDir == "" {
		return nil
	}

	log.Info("Loading custom schema...")

	if !isDir(db.SchemaDir) {
		return fmt.Errorf("LoadSchema(): SchemaDir does not exist")
	}

	for _, kpb := range db.Topology.Keyspaces {
		keyspace := kpb.Name
		keyspaceDir := path.Join(db.SchemaDir, keyspace)

		schemaDir := keyspaceDir
		if !isDir(schemaDir) {
			schemaDir = db.DefaultSchemaDir
			if schemaDir == "" || !isDir(schemaDir) {
				return fmt.Errorf("LoadSchema: schema dir for ks `%s` does not exist (%s)", keyspace, schemaDir)
			}
		}

		if shouldRunDatabaseMigrations {
			glob, _ := filepath.Glob(path.Join(schemaDir, "*.sql"))
			for _, filepath := range glob {
				cmds, err := LoadSQLFile(filepath, schemaDir)
				if err != nil {
					return err
				}

				// One single vschema migration per file
				if !db.OnlyMySQL && len(cmds) == 1 && strings.HasPrefix(strings.ToUpper(cmds[0]), "ALTER VSCHEMA") {
					if err = db.applyVschema(keyspace, cmds[0]); err != nil {
						return err
					}
					continue
				}

				for _, dbname := range db.shardNames(kpb) {
					if err := db.Execute(cmds, dbname); err != nil {
						return err
					}
				}
			}
		}

		if !db.OnlyMySQL {
			if err := db.reloadSchemaKeyspace(keyspace); err != nil {
				return err
			}
		}
	}

	return nil
}

func (db *LocalCluster) createVTSchema() error {
	var sidecardbExec sidecardb.Exec = func(ctx context.Context, query string, maxRows int, useDB bool) (*sqltypes.Result, error) {
		if useDB {
			if err := db.Execute([]string{fmt.Sprintf("use %s", sidecar.GetIdentifier())}, ""); err != nil {
				return nil, err
			}
		}
		return db.ExecuteFetch(query, "")
	}

	if err := sidecardb.Init(context.Background(), vtenv.NewTestEnv(), sidecardbExec); err != nil {
		return err
	}
	return nil
}

func (db *LocalCluster) createDatabases() error {
	log.Info("Creating databases in cluster...")

	// The tablets created in vttest do not follow the same tablet init process, so we need to explicitly create
	// the sidecar database tables
	if err := db.createVTSchema(); err != nil {
		return err
	}

	var sql []string
	for _, kpb := range db.Topology.Keyspaces {
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
		_, err := conn.ExecuteFetch(cmd, -1, false)
		if err != nil {
			return err
		}
	}

	_, err = conn.ExecuteFetch("COMMIT", 0, false)
	return err
}

// ExecuteFetch runs a SQL statement on the MySQL instance backing
// this local cluster and returns the result.
func (db *LocalCluster) ExecuteFetch(sql string, dbname string) (*sqltypes.Result, error) {
	params := db.mysql.Params(dbname)
	conn, err := mysql.Connect(context.Background(), &params)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	log.Infof("ExecuteFetch(%s): \"%s\"", dbname, sql)
	rs, err := conn.ExecuteFetch(sql, -1, true)
	return rs, err
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
func (db *LocalCluster) JSONConfig() any {
	if db.OnlyMySQL {
		return db.mysql.Params("")
	}

	config := map[string]any{
		"bind_address":       db.vt.BindAddress,
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

// GrpcPort returns the grpc port used by vtcombo
func (db *LocalCluster) GrpcPort() int {
	return db.vt.PortGrpc
}

func (db *LocalCluster) applyVschema(keyspace string, migration string) error {
	server := fmt.Sprintf("localhost:%v", db.vt.PortGrpc)
	args := []string{"ApplyVSchema", "--sql", migration, keyspace}
	fmt.Printf("Applying vschema %v", args)
	err := vtctlclient.RunCommandAndWait(context.Background(), server, args, func(e *logutil.Event) {
		log.Info(e)
	})

	return err
}

func (db *LocalCluster) reloadSchemaKeyspace(keyspace string) error {
	server := fmt.Sprintf("localhost:%v", db.vt.PortGrpc)
	args := []string{"ReloadSchemaKeyspace", "--include_primary=true", keyspace}
	log.Infof("Reloading keyspace schema %v", args)

	err := vtctlclient.RunCommandAndWait(context.Background(), server, args, func(e *logutil.Event) {
		log.Info(e)
	})

	return err
}

func dirExist(dir string) bool {
	exist := true
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		exist = false
	}
	return exist
}

// LoadSQLFile loads a parses a .sql file from disk, removing all the
// different comments that mysql/mysqldump inserts in these, and returning
// each individual SQL statement as its own string.
// If sourceroot is set, that directory will be used when resolving `source `
// statements in the SQL file.
func LoadSQLFile(filename, sourceroot string) ([]string, error) {
	var (
		cmd  strings.Builder
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

func (db *LocalCluster) VTProcess() *VtProcess {
	return db.vt
}

// ReadVSchema reads the vschema from the vtgate endpoint for it and returns
// a pointer to the interface. To read this vschema, the caller must convert it to a map
func (vt *VtProcess) ReadVSchema() (*interface{}, error) {
	httpClient := &http.Client{Timeout: 5 * time.Second}
	resp, err := httpClient.Get(fmt.Sprintf("http://%s:%d/debug/vschema", vt.BindAddress, vt.Port))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	res, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	var results interface{}
	err = json.Unmarshal(res, &results)
	if err != nil {
		return nil, err
	}
	return &results, nil
}
