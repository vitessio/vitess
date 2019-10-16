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
	"time"

	"vitess.io/vitess/go/exit"
	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/vt/binlog/binlogplayer"
	"vitess.io/vitess/go/vt/dbconfigs"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/mysqlctl"
	"vitess.io/vitess/go/vt/servenv"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vttablet/tabletmanager/vreplication"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
)

var (
	vtShovelConfigFile = flag.String("vtshovel-config-file", "/etc/slack.d/vtshovel.json", "VTShovel Config file")
	dryRun             = flag.Bool("dry-run", false, "When present, only log DML that are going to be performed in target database")
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
}

func main() {
	defer exit.Recover()

	dbconfigs.RegisterFlags(dbconfigs.Dba)
	mysqlctl.RegisterFlags()

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

	sourceConnParams := mysql.ConnParams{
		Host:   vtShovelConfig.MySQLSourceHost,
		Port:   vtShovelConfig.MySQLSourcePort,
		Pass:   vtShovelConfig.MySQLSourcePassword,
		Uname:  vtShovelConfig.MySQLSourceUser,
		DbName: vtShovelConfig.MySQLSourceDBName,
	}

	source := binlogdatapb.BinlogSource{
		Filter: &binlogdatapb.Filter{
			Rules: []*binlogdatapb.Rule{
				&binlogdatapb.Rule{
					Match: "/.*",
				},
			},
		},
	}

	var mycnf *mysqlctl.Mycnf
	var socketFile string
	// If no connection parameters were specified, load the mycnf file
	// and use the socket from it. If connection parameters were specified,
	// we assume that the mysql is not local, and we skip loading mycnf.
	// This also means that backup and restore will not be allowed.
	if !dbconfigs.HasConnectionParams() {
		var err error
		if mycnf, err = mysqlctl.NewMycnfFromFlags(123213123); err != nil {
			log.Exitf("mycnf read failed: %v", err)
		}
		socketFile = mycnf.SocketFile
	} else {
		log.Info("connection parameters were specified. Not loading my.cnf.")
	}

	// If connection parameters were specified, socketFile will be empty.
	// Otherwise, the socketFile (read from mycnf) will be used to initialize
	// dbconfigs.
	dbcfgs, err := dbconfigs.Init(socketFile)
	if err != nil {
		log.Warning(err)
	}

	mysqld := mysqlctl.NewMysqld(dbcfgs)
	servenv.OnClose(mysqld.Close)

	destConnParams := dbcfgs.Dba()
	// Hack to make sure dbname is set correctly given that this is not a tablet
	// and SetDBName is not called.
	destConnParams.DbName = destConnParams.DeprecatedDBName

	log.Infof("This are the destConnParams:%v", destConnParams)
	destDbClient := binlogplayer.NewDBClient(destConnParams)

	if err := destDbClient.Connect(); err != nil {
		log.Fatal(vterrors.Wrap(err, "can't connect to database"))
	}
	servenv.OnClose(destDbClient.Close)

	for _, query := range binlogplayer.CreateVReplicationTable() {
		if _, err := destDbClient.ExecuteFetch(query, 0); err != nil {
			log.Fatalf("Failed to ensure vreplication table exists: %v", err)
		}
	}

	newVReplicatorStmt := binlogplayer.CreateVReplication("VTshovel", &source, vtShovelConfig.MySQLSourceBinlogStartPos, int64(1000), int64(100000), time.Now().Unix(), destDbClient.DBName())

	res, err := destDbClient.ExecuteFetch(newVReplicatorStmt, 0)
	if err != nil {
		log.Fatalf("Failed to create vreplication stream: %v", err)
	}

	sourceVstreamClient := vreplication.NewMySQLVStreamerClient(&sourceConnParams)

	go func() {
		ctx := context.Background()
		replicator := vreplication.NewVReplicator(
			uint32(res.InsertID),
			&source,
			sourceVstreamClient,
			binlogplayer.NewStats(),
			destDbClient,
			mysqld,
		)
		replicator.Replicate(ctx)
		if err != nil {
			log.Infof("Error with stream: %v", err)

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
