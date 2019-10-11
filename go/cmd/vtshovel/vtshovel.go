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

package main

import (
	"context"
	"encoding/json"
	"flag"
	"io/ioutil"
	"math/rand"
	"regexp"
	"strings"
	"time"

	"vitess.io/vitess/go/exit"
	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/vt/binlog/binlogplayer"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/servenv"
	"vitess.io/vitess/go/vt/vterrors"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/vttablet/tabletmanager/vreplication"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
)

var (
	vtShovelConfigFile = flag.String("vtshovel-config-file", "/etc/slack.d/vtshovel.json", "VTShovel Config file")
	dryRun             = flag.Bool("dry-run", false, "When present, only log DML that are going to be performed in target database")

	autoIncr = regexp.MustCompile(` AUTO_INCREMENT=\d+`)
)

func init() {
	rand.Seed(time.Now().UnixNano())
	servenv.RegisterDefaultFlags()
}

// VtShovelConfig fields to configure vtshovel client
type VtShovelConfig struct {
	// Source MySQL client information

	// MySQLSourceHost ...
	MySQLSourceHost string `json:"mysql_source_host"`
	// MySQLSourcePort ...
	MySQLSourcePort int `json:"mysql_source_port"`
	// MySQLSourceUser ...
	MySQLSourceUser string `json:"mysql_source_user"`
	// MySQLSourcePassword ...
	MySQLSourcePassword string `json:"mysql_source_password"`
	// MySQLSourceBinlogStartPos ...
	MySQLSourceBinlogStartPos string `json:"mysql_source_binlog_start_pos"`
	// MySQLSourceDatabase ...
	MySQLSourceDBName string `json:"mysql_source_dbname"`

	// Target MySQL client information

	// MySQLTargetHost ...
	MySQLTargetHost string `json:"mysql_target_host"`
	// MySQLTargetPort ...
	MySQLTargetPort int `json:"mysql_target_port"`
	// MySQLTargetUser ...
	MySQLTargetUser string `json:"mysql_target_user"`
	// MySQLTargetPassword ...
	MySQLTargetPassword string `json:"mysql_target_password"`
	// MySQLTargetDBName ...
	MySQLTargetDBName string `json:"mysql_target_dbname"`
}

func main() {
	defer exit.Recover()

	servenv.ParseFlags("vtshovel")
	servenv.Init()

	servenv.OnRun(func() {
		//vreplication.MySQLAddStatusPart()
		// Flags are parsed now. Parse the template using the actual flag value and overwrite the current template.
		//addStatusParts(vtg)
	})

	vtShovelConfig, err := loadConfigFromFile(*vtShovelConfigFile)
	if err != nil {
		log.Fatal(err)
	}

	targetConnParams := mysql.ConnParams{
		Host:   vtShovelConfig.MySQLTargetHost,
		Port:   vtShovelConfig.MySQLTargetPort,
		Pass:   vtShovelConfig.MySQLTargetPassword,
		Uname:  vtShovelConfig.MySQLTargetUser,
		DbName: vtShovelConfig.MySQLTargetDBName,
	}
	dbTargetClient := newVtShovelDbClient(
		binlogplayer.NewDBClient(&targetConnParams),
		vtShovelConfig.MySQLSourceBinlogStartPos,
	)

	if err := dbTargetClient.Connect(); err != nil {
		log.Fatal(vterrors.Wrap(err, "can't connect to database"))
	}

	sourceConnParams := mysql.ConnParams{
		Host:  vtShovelConfig.MySQLSourceHost,
		Port:  vtShovelConfig.MySQLSourcePort,
		Pass:  vtShovelConfig.MySQLSourcePassword,
		Uname: vtShovelConfig.MySQLSourceUser,
	}

	servenv.OnClose(dbTargetClient.Close)

	source := binlogdatapb.BinlogSource{
		Filter: &binlogdatapb.Filter{
			Rules: []*binlogdatapb.Rule{
				&binlogdatapb.Rule{
					Match: "/" + vtShovelConfig.MySQLSourceDBName + ".*/",
				},
			},
		},
	}
	ctx := context.Background()
	sourceVstreamClient := vreplication.NewMySQLVStreamerClient(&sourceConnParams)
	go func() {
		replicator := vreplication.NewVReplicator(
			1,
			&source,
			sourceVstreamClient,
			binlogplayer.NewStats(),
			dbTargetClient,
			newVtShovelSchemaLoader(),
		)
		replicator.Replicate(ctx)
		if err != nil {
			log.Infof("Error starting stream: %v", err)

		}
		return
	}()
	servenv.RunDefault()
}

func loadConfigFromFile(file string) (*VtShovelConfig, error) {
	data, err := ioutil.ReadFile(file)
	if err != nil {
		return nil, vterrors.Wrapf(err, "Failed to read %v file", file)
	}
	vtShovelConfig := &VtShovelConfig{}
	err = json.Unmarshal(data, vtShovelConfig)
	if err != nil {
		return nil, vterrors.Wrap(err, "Error parsing auth server config")
	}
	return vtShovelConfig, nil
}

type vtShovelDbClient struct {
	dbClient binlogplayer.DBClient
	startPos string
}

type vtShovelSchemaLoader struct{}

func newVtShovelDbClient(dbClient binlogplayer.DBClient, startPos string) binlogplayer.DBClient {
	return &vtShovelDbClient{
		dbClient: dbClient,
		startPos: startPos,
	}
}

func newVtShovelSchemaLoader() vreplication.SchemasLoader {
	return &vtShovelSchemaLoader{}
}

func (vdc *vtShovelDbClient) DBName() string {
	return vdc.dbClient.DBName()
}

func (vdc *vtShovelDbClient) Connect() error {
	return vdc.dbClient.Connect()
}

func (vdc *vtShovelDbClient) Begin() error {
	return vdc.dbClient.Begin()
}

func (vdc *vtShovelDbClient) Commit() error {
	return vdc.dbClient.Commit()
}

func (vdc *vtShovelDbClient) Rollback() error {
	return vdc.dbClient.Rollback()
}

func (vdc *vtShovelDbClient) Close() {
	vdc.dbClient.Close()
}

func (vdc *vtShovelDbClient) ExecuteFetch(query string, maxrows int) (*sqltypes.Result, error) {
	if strings.Contains(query, "from _vt.copy_state") {
		dummyResult := &sqltypes.Result{
			Rows: [][]sqltypes.Value{
				[]sqltypes.Value{
					sqltypes.NewInt64(0),
				},
			},
		}
		return dummyResult, nil
	}

	if strings.Contains(query, "from _vt.vreplication") {
		dummyResult := &sqltypes.Result{
			Rows: [][]sqltypes.Value{
				[]sqltypes.Value{
					sqltypes.NewVarBinary(vdc.startPos),
					sqltypes.NewVarBinary(""),        // StopPos
					sqltypes.NewInt64(10000),         // maxTPS
					sqltypes.NewInt64(10000),         // maxReplicationLag
					sqltypes.NewVarBinary("Running"), // state
				},
			},
		}
		return dummyResult, nil
	}

	if strings.Contains(query, "update _vt.vreplication") {
		return &sqltypes.Result{}, nil
	}
	return vdc.dbClient.ExecuteFetch(query, maxrows)
}

func (vsl *vtShovelSchemaLoader) GetSchema(dbName string, tables, excludeTables []string, includeViews bool) (*tabletmanagerdatapb.SchemaDefinition, error) {
	// TODO: This will only work for stament based replication.
	return &tabletmanagerdatapb.SchemaDefinition{
		TableDefinitions: []*tabletmanagerdatapb.TableDefinition{},
	}, nil
}
