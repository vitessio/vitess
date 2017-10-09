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
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"strings"

	log "github.com/golang/glog"

	"github.com/youtube/vitess/go/mysql"
	vttestpb "github.com/youtube/vitess/go/vt/proto/vttest"
)

// Handle allows you to interact with the processes launched by vttest.
// Deprecated: this is a legacy interface kept for backwards compatibility
// reasons. Consider using LocalCluster directly instead.
type Handle struct {
	db *LocalCluster
}

// VtgateAddress returns the address under which vtgate is reachable e.g.
// "localhost:15991".
func (hdl *Handle) VtgateAddress() (string, error) {
	proto := hdl.db.Env.DefaultProtocol()
	port := hdl.db.Env.PortForProtocol("vtcombo", proto)
	return fmt.Sprintf("localhost:%d", port), nil
}

// VitessOption is the type for generic options to be passed in to LaunchVitess.
// Deprecated: this is a legacy interface kept for backwards compatibility
// reasons. Consider setting the Config struct in LocalCluster instead.
type VitessOption struct {
	beforeRun func(*Handle) error
	afterRun  func()
}

// Verbose makes the underlying local_cluster verbose.
// Deprecated: See LocalCluster, Config
func Verbose(verbose bool) VitessOption {
	return VitessOption{
		beforeRun: func(hdl *Handle) error {
			if verbose {
				flag.Set("alsologtostderr", "true")
				flag.Set("v", "5")
			}
			return nil
		},
	}
}

// NoStderr makes the underlying local_cluster stderr output disapper.
// Deprecated: See LocalCluster, Config
func NoStderr() VitessOption {
	return VitessOption{
		beforeRun: func(hdl *Handle) error {
			flag.Set("alsologtostderr", "false")
			return nil
		},
	}
}

// SchemaDirectory is used to specify a directory to read schema from.
// It cannot be used at the same time as Schema.
// Deprecated: See LocalCluster, Config
func SchemaDirectory(dir string) VitessOption {
	if dir == "" {
		log.Fatal("BUG: provided directory must not be empty")
	}

	return VitessOption{
		beforeRun: func(hdl *Handle) error {
			if hdl.db.SchemaDir != "" {
				return fmt.Errorf("SchemaDirectory option (%v) would overwrite directory set by another option (%v)", dir, hdl.db.SchemaDir)
			}
			hdl.db.SchemaDir = dir
			return nil
		},
	}
}

// ProtoTopo is used to pass in the topology as a vttest proto definition.
// See vttest.proto for more information.
// It cannot be used at the same time as MySQLOnly.
// Deprecated: See LocalCluster, Config
func ProtoTopo(topo *vttestpb.VTTestTopology) VitessOption {
	return VitessOption{
		beforeRun: func(hdl *Handle) error {
			if hdl.db.DbName() != "" {
				return fmt.Errorf("duplicate MySQLOnly option or conflicting ProtoTopo option. You can only use one")
			}

			hdl.db.Topology = topo
			return nil
		},
	}
}

// MySQLOnly is used to launch only a mysqld instance, with the specified db name.
// It cannot be used at the same as ProtoTopo.
// Deprecated: See LocalCluster, Config
func MySQLOnly(dbName string) VitessOption {
	return VitessOption{
		beforeRun: func(hdl *Handle) error {
			if hdl.db.DbName() != "" {
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

			hdl.db.Topology = topo
			hdl.db.OnlyMySQL = true
			return nil
		},
	}
}

// Schema is used to specify SQL commands to run at startup.
// It conflicts with SchemaDirectory.
// This option requires a ProtoTopo or MySQLOnly option before.
// Deprecated: See LocalCluster, Config
func Schema(schema string) VitessOption {
	if schema == "" {
		log.Fatal("BUG: provided schema must not be empty")
	}

	tempSchemaDir := ""
	return VitessOption{
		beforeRun: func(hdl *Handle) error {
			if hdl.db.DbName() == "" {
				return fmt.Errorf("Schema option requires a previously passed MySQLOnly option")
			}
			if hdl.db.SchemaDir != "" {
				return fmt.Errorf("Schema option would overwrite directory set by another option (%v)", hdl.db.SchemaDir)
			}

			var err error
			tempSchemaDir, err = ioutil.TempDir("", "vt")
			if err != nil {
				return err
			}
			ksDir := path.Join(tempSchemaDir, hdl.db.DbName())
			err = os.Mkdir(ksDir, os.ModeDir|0775)
			if err != nil {
				return err
			}
			fileName := path.Join(ksDir, "schema.sql")
			err = ioutil.WriteFile(fileName, []byte(schema), 0666)
			if err != nil {
				return err
			}
			hdl.db.SchemaDir = tempSchemaDir
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
// Deprecated: See LocalCluster, Config
func VSchema(vschema interface{}) VitessOption {
	if vschema == "" {
		log.Fatal("BUG: provided vschema object must not be nil")
	}

	vschemaFilePath := ""
	return VitessOption{
		beforeRun: func(hdl *Handle) error {
			if hdl.db.SchemaDir == "" {
				return errors.New("VSchema option must be specified after a Schema or SchemaDirectory option")
			}

			vschemaFilePath := path.Join(hdl.db.SchemaDir, "vschema.json")
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
// Deprecated: See LocalCluster, Config
func ExtraMyCnf(extraMyCnf string) VitessOption {
	return VitessOption{
		beforeRun: func(hdl *Handle) error {
			hdl.db.ExtraMyCnf = strings.Split(extraMyCnf, ":")
			return nil
		},
	}
}

// InitDataOptions contain the command line arguments that configure
// initialization of vttest with random data.
// Deprecated: See LocalCluster, Config
type InitDataOptions struct{}

// InitData returns a VitessOption that sets the InitDataOptions parameters.
// Deprecated: This interface has never really worked because InitDataOptions
// is not properly exported. See SeedData, LocalCluster, Config
func InitData(i *InitDataOptions) VitessOption {
	return VitessOption{
		beforeRun: func(hdl *Handle) error {
			return fmt.Errorf("InitData is not supported. Use SeedData instead")
		},
	}
}

// SeedData returns a VitessOption that sets the InitDataOptions parameters.
// Deprecated: See LocalCluster, Config
func SeedData(seed *SeedConfig) VitessOption {
	return VitessOption{
		beforeRun: func(hdl *Handle) error {
			hdl.db.Seed = seed
			return nil
		},
	}
}

// LaunchVitess launches a vitess test cluster.
// Deprecated: this is a legacy interface kept for backwards compatibility
// reasons. Consider using LocalCluster directly instead.
func LaunchVitess(options ...VitessOption) (hdl *Handle, err error) {
	hdl = &Handle{&LocalCluster{}}
	err = hdl.run(options...)
	return
}

// TearDown tears down the launched processes.
// Deprecated: See LocalCluster, Config
func (hdl *Handle) TearDown() error {
	return hdl.db.TearDown()
}

// MySQLConnParams builds the MySQL connection params.
// It's valid only if you used MySQLOnly option.
// Deprecated: See LocalCluster, Config
func (hdl *Handle) MySQLConnParams() (mysql.ConnParams, error) {
	return hdl.db.MySQLConnParams(), nil
}

// MySQLAppDebugConnParams builds the MySQL connection params for appdebug user.
// It's valid only if you used MySQLOnly option.
// Deprecated: See LocalCluster, Config
func (hdl *Handle) MySQLAppDebugConnParams() (mysql.ConnParams, error) {
	connParams, err := hdl.MySQLConnParams()
	connParams.Uname = "vt_appdebug"
	return connParams, err
}

func (hdl *Handle) run(options ...VitessOption) error {
	for _, option := range options {
		if err := option.beforeRun(hdl); err != nil {
			return err
		}
		if option.afterRun != nil {
			defer option.afterRun()
		}
	}

	return hdl.db.Setup()
}
