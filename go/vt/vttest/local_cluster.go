// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package vttest provides the functionality to bring
// up a test cluster.
package vttest

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"os"
	"os/exec"
	"path"
	"strconv"
	"strings"
	"time"

	log "github.com/golang/glog"
	"github.com/golang/protobuf/proto"
	"github.com/youtube/vitess/go/sqldb"

	vttestpb "github.com/youtube/vitess/go/vt/proto/vttest"
)

// Handle allows you to interact with the processes launched by vttest.
type Handle struct {
	Data map[string]interface{}

	cmd   *exec.Cmd
	stdin io.WriteCloser

	// dbname is valid only for LaunchMySQL.
	dbname string
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
	return VitessOption{
		beforeRun: func(hdl *Handle) error {
			if dir != "" {
				hdl.cmd.Args = append(hdl.cmd.Args, "--schema_dir", dir)
			}
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
			hdl.cmd.Args = append(hdl.cmd.Args, "--proto_topo", proto.CompactTextString(topo))
			return nil
		},
	}
}

// MySQLOnly is used to launch only a mysqld instance, with the specified db name.
// Use it before Schema option.
// It cannot be used at the same as ProtoTopo.
func MySQLOnly(dbName string) VitessOption {
	return VitessOption{
		beforeRun: func(hdl *Handle) error {
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
// It conflicts with SchemaDirectory
func Schema(schema string) VitessOption {
	schemaDir := ""
	return VitessOption{
		beforeRun: func(hdl *Handle) error {
			if schema == "" {
				return nil
			}
			if hdl.dbname == "" {
				return fmt.Errorf("Schema option requires a previously passed MySQLOnly option")
			}
			var err error
			schemaDir, err = ioutil.TempDir("", "vt")
			if err != nil {
				return err
			}
			ksDir := path.Join(schemaDir, hdl.dbname)
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
			hdl.cmd.Args = append(hdl.cmd.Args, "--schema_dir", schemaDir)
			return nil
		},
		afterRun: func() {
			if schemaDir != "" {
				os.RemoveAll(schemaDir)
			}
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
func (hdl *Handle) MySQLConnParams() (sqldb.ConnParams, error) {
	params := sqldb.ConnParams{
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

// randomPort returns a random number between 10k & 30k.
func randomPort() int {
	v := rand.Int31n(20000)
	return int(v + 10000)
}

func init() {
	rand.Seed(time.Now().UnixNano())
}
