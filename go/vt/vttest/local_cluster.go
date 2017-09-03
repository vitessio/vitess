/*
Copyright 2017 Google Inc.

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

// Package vttest provides the functionality to bring
// up a test cluster.
package vttest

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"os/exec"
	"path"
	"strconv"
	"strings"

	log "github.com/golang/glog"
	"github.com/golang/protobuf/proto"

	"github.com/youtube/vitess/go/mysql"
	vttestpb "github.com/youtube/vitess/go/vt/proto/vttest"
)

// Handle allows you to interact with the processes launched by vttest.
type Handle struct {
	Data map[string]interface{}

	cmd   *exec.Cmd
	stdin io.WriteCloser

	// schemaDir is the directory which will be passed as --schema_dir flag.
	// We store it here because the VSchema() option must reference it.
	schemaDir string

	// dbname is valid only for LaunchMySQL.
	dbname string
}

// VtgateAddress returns the address under which vtgate is reachable e.g.
// "localhost:15991".
// An error is returned if the data is not available.
func (hdl *Handle) VtgateAddress() (string, error) {
	if hdl.Data == nil {
		return "", errors.New("Data field in Handle is empty")
	}
	portName := "port"
	if vtgateProtocol() == "grpc" {
		portName = "grpc_port"
	}
	fport, ok := hdl.Data[portName]
	if !ok {
		return "", fmt.Errorf("port %v not found in map", portName)
	}
	port := int(fport.(float64))
	return fmt.Sprintf("localhost:%d", port), nil
}

// VitessOption is the type for generic options to be passed in to LaunchVitess.
type VitessOption struct {
	// beforeRun is executed before we start run_local_database.py.
	beforeRun func(*Handle) error

	// afterRun is executed after run_local_database.py has been
	// started and is running (and is done reading and applying
	// the schema).
	afterRun func()
}

// Verbose makes the underlying local_cluster verbose.
func Verbose(verbose bool) VitessOption {
	return VitessOption{
		beforeRun: func(hdl *Handle) error {
			if verbose {
				hdl.cmd.Args = append(hdl.cmd.Args, "--verbose")
			}
			return nil
		},
	}
}

// NoStderr makes the underlying local_cluster stderr output disapper.
func NoStderr() VitessOption {
	return VitessOption{
		beforeRun: func(hdl *Handle) error {
			hdl.cmd.Stderr = nil
			return nil
		},
	}
}

// SchemaDirectory is used to specify a directory to read schema from.
// It cannot be used at the same time as Schema.
func SchemaDirectory(dir string) VitessOption {
	if dir == "" {
		log.Fatal("BUG: provided directory must not be empty")
	}

	return VitessOption{
		beforeRun: func(hdl *Handle) error {
			if hdl.schemaDir != "" {
				return fmt.Errorf("SchemaDirectory option (%v) would overwrite directory set by another option (%v)", dir, hdl.schemaDir)
			}
			hdl.schemaDir = dir
			return nil
		},
	}
}

// ProtoTopo is used to pass in the topology as a vttest proto definition.
// See vttest.proto for more information.
// It cannot be used at the same time as MySQLOnly.
func ProtoTopo(topo *vttestpb.VTTestTopology) VitessOption {
	return VitessOption{
		beforeRun: func(hdl *Handle) error {
			if hdl.dbname != "" {
				return fmt.Errorf("duplicate MySQLOnly option or conflicting ProtoTopo option. You can only use one")
			}

			if len(topo.GetKeyspaces()) > 0 {
				hdl.dbname = topo.GetKeyspaces()[0].Name
			}
			hdl.cmd.Args = append(hdl.cmd.Args, "--proto_topo", proto.CompactTextString(topo))
			return nil
		},
	}
}

// MySQLOnly is used to launch only a mysqld instance, with the specified db name.
// It cannot be used at the same as ProtoTopo.
func MySQLOnly(dbName string) VitessOption {
	return VitessOption{
		beforeRun: func(hdl *Handle) error {
			if hdl.dbname != "" {
				return fmt.Errorf("duplicate MySQLOnly option or conflicting ProtoTopo option. You can only use one")
			}

			// the way to pass the dbname for creation in
			// is to provide a topology
			topo := &vttestpb.VTTestTopology{
				Keyspaces: []*vttestpb.Keyspace{
					{
						Name: dbName,
						Shards: []*vttestpb.Shard{
							{
								Name:           "0",
								DbNameOverride: dbName,
							},
						},
					},
				},
			}

			hdl.dbname = dbName
			hdl.cmd.Args = append(hdl.cmd.Args,
				"--mysql_only",
				"--proto_topo", proto.CompactTextString(topo))
			return nil
		},
	}
}

// Schema is used to specify SQL commands to run at startup.
// It conflicts with SchemaDirectory.
// This option requires a ProtoTopo or MySQLOnly option before.
func Schema(schema string) VitessOption {
	if schema == "" {
		log.Fatal("BUG: provided schema must not be empty")
	}

	tempSchemaDir := ""
	return VitessOption{
		beforeRun: func(hdl *Handle) error {
			if hdl.dbname == "" {
				return fmt.Errorf("Schema option requires a previously passed MySQLOnly option")
			}
			if hdl.schemaDir != "" {
				return fmt.Errorf("Schema option would overwrite directory set by another option (%v)", hdl.schemaDir)
			}

			var err error
			tempSchemaDir, err = ioutil.TempDir("", "vt")
			if err != nil {
				return err
			}
			ksDir := path.Join(tempSchemaDir, hdl.dbname)
			err = os.Mkdir(ksDir, os.ModeDir|0775)
			if err != nil {
				return err
			}
			fileName := path.Join(ksDir, "schema.sql")
			f, err := os.Create(fileName)
			if err != nil {
				return err
			}
			n, err := f.WriteString(schema)
			if n != len(schema) {
				return errors.New("short write")
			}
			if err != nil {
				return err
			}
			err = f.Close()
			if err != nil {
				return err
			}
			hdl.schemaDir = tempSchemaDir
			return nil
		},
		afterRun: func() {
			if tempSchemaDir != "" {
				os.RemoveAll(tempSchemaDir)
			}
		},
	}
}

// VSchema is used to create a vschema.json file in the --schema_dir directory.
// It must be used *after* the Schema or SchemaDirectory option was provided.
func VSchema(vschema interface{}) VitessOption {
	if vschema == "" {
		log.Fatal("BUG: provided vschema object must not be nil")
	}

	vschemaFilePath := ""
	return VitessOption{
		beforeRun: func(hdl *Handle) error {
			if hdl.schemaDir == "" {
				return errors.New("VSchema option must be specified after a Schema or SchemaDirectory option")
			}

			vschemaFilePath := path.Join(hdl.schemaDir, "vschema.json")
			if _, err := os.Stat(vschemaFilePath); err == nil {
				return fmt.Errorf("temporary vschema.json already exists at %v. delete it first", vschemaFilePath)
			}

			vschemaJSON, err := json.Marshal(vschema)
			if err != nil {
				return err
			}
			if err := ioutil.WriteFile(vschemaFilePath, vschemaJSON, 0644); err != nil {
				return err
			}
			return nil
		},
		afterRun: func() {
			os.Remove(vschemaFilePath)
		},
	}
}

// ExtraMyCnf adds one or more 'my.cnf'-style config files to MySQL.
// (if more than one, the ':' separator should be used).
func ExtraMyCnf(extraMyCnf string) VitessOption {
	return VitessOption{
		beforeRun: func(hdl *Handle) error {
			hdl.cmd.Args = append(hdl.cmd.Args, "--extra_my_cnf", extraMyCnf)
			return nil
		},
	}
}

// InitDataOptions contain the command line arguments that configure
// initialization of vttest with random data. See the documentation of
// the corresponding command line flags in py/vttest/run_local_database.py
// for the meaning of each field. If a field is nil, the flag will not be
// specified when running 'run_local_database.py' and the default value for
// the flag will be used.
type InitDataOptions struct {
	rngSeed           *int
	minTableShardSize *int
	maxTableShardSize *int
	nullProbability   *float64
}

// InitData returns a VitessOption that sets the InitDataOptions parameters.
func InitData(i *InitDataOptions) VitessOption {
	return VitessOption{
		beforeRun: func(hdl *Handle) error {
			hdl.cmd.Args = append(hdl.cmd.Args, "--initialize_with_random_data")
			if i.rngSeed != nil {
				hdl.cmd.Args = append(hdl.cmd.Args, "--rng_seed", fmt.Sprintf("%v", *i.rngSeed))
			}
			if i.minTableShardSize != nil {
				hdl.cmd.Args = append(hdl.cmd.Args, "--min_table_shard_size", fmt.Sprintf("%v", *i.minTableShardSize))
			}
			if i.maxTableShardSize != nil {
				hdl.cmd.Args = append(hdl.cmd.Args, "--max_table_shard_size", fmt.Sprintf("%v", *i.maxTableShardSize))
			}
			if i.nullProbability != nil {
				hdl.cmd.Args = append(hdl.cmd.Args, "--null_probability", fmt.Sprintf("%v", *i.nullProbability))
			}
			return nil
		},
	}
}

// LaunchVitess launches a vitess test cluster.
func LaunchVitess(
	options ...VitessOption,
) (hdl *Handle, err error) {
	hdl = &Handle{}
	err = hdl.run(options...)
	if err != nil {
		return nil, err
	}
	return hdl, nil
}

// TearDown tears down the launched processes.
func (hdl *Handle) TearDown() error {
	_, err := hdl.stdin.Write([]byte("\n"))
	if err != nil {
		return err
	}
	return hdl.cmd.Wait()
}

// MySQLConnParams builds the MySQL connection params.
// It's valid only if you used MySQLOnly option.
func (hdl *Handle) MySQLConnParams() (mysql.ConnParams, error) {
	params := mysql.ConnParams{
		Charset: "utf8",
		DbName:  hdl.dbname,
	}
	if hdl.Data == nil {
		return params, errors.New("no data")
	}
	iuser, ok := hdl.Data["username"]
	if !ok {
		return params, errors.New("no username")
	}
	user, ok := iuser.(string)
	if !ok {
		return params, fmt.Errorf("invalid user type: %T", iuser)
	}
	params.Uname = user
	if ipassword, ok := hdl.Data["password"]; ok {
		password, ok := ipassword.(string)
		if !ok {
			return params, fmt.Errorf("invalid password type: %T", ipassword)
		}
		params.Pass = password
	}
	if ihost, ok := hdl.Data["host"]; ok {
		host, ok := ihost.(string)
		if !ok {
			return params, fmt.Errorf("invalid host type: %T", ihost)
		}
		params.Host = host
	}
	if iport, ok := hdl.Data["port"]; ok {
		port, ok := iport.(float64)
		if !ok {
			return params, fmt.Errorf("invalid port type: %T", iport)
		}
		params.Port = int(port)
	}
	if isocket, ok := hdl.Data["socket"]; ok {
		socket, ok := isocket.(string)
		if !ok {
			return params, fmt.Errorf("invalid socket type: %T", isocket)
		}
		params.UnixSocket = socket
	}
	return params, nil
}

// MySQLAppDebugConnParams builds the MySQL connection params for appdebug user.
// It's valid only if you used MySQLOnly option.
func (hdl *Handle) MySQLAppDebugConnParams() (mysql.ConnParams, error) {
	connParams, err := hdl.MySQLConnParams()
	connParams.Uname = "vt_appdebug"
	return connParams, err
}

func (hdl *Handle) run(
	options ...VitessOption,
) error {
	launcher, err := launcherPath()
	if err != nil {
		return err
	}
	port := randomPort()
	hdl.cmd = exec.Command(
		launcher,
		"--port", strconv.Itoa(port),
	)
	hdl.cmd.Stderr = os.Stderr

	for _, option := range options {
		if err := option.beforeRun(hdl); err != nil {
			return err
		}
		if option.afterRun != nil {
			defer option.afterRun()
		}
	}
	if hdl.schemaDir != "" {
		hdl.cmd.Args = append(hdl.cmd.Args, "--schema_dir", hdl.schemaDir)
	}

	log.Infof("executing: %v", strings.Join(hdl.cmd.Args, " "))

	stdout, err := hdl.cmd.StdoutPipe()
	if err != nil {
		return err
	}
	decoder := json.NewDecoder(stdout)
	hdl.stdin, err = hdl.cmd.StdinPipe()
	if err != nil {
		return err
	}
	err = hdl.cmd.Start()
	if err != nil {
		return err
	}
	err = decoder.Decode(&hdl.Data)
	if err != nil {
		err = fmt.Errorf(
			"error (%v) parsing JSON output from command: %v", err, launcher)
	}
	return err
}
