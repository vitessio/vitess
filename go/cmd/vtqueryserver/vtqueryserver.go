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
	"flag"
	"os"

	"vitess.io/vitess/go/trace"
	"vitess.io/vitess/go/vt/dbconfigs"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/servenv"
	"vitess.io/vitess/go/vt/vtqueryserver"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/tabletenv"
)

var (
	dbName          string
	mysqlSocketFile = flag.String("mysql-socket-file", "", "path to unix socket file to connect to mysql")
)

func init() {
	servenv.RegisterDefaultFlags()
	// TODO(demmer): remove once migrated to using db_name
	flag.StringVar(&dbName, "db-config-app-dbname", "", "db connection dbname")
	flag.StringVar(&dbName, "db_name", "", "db connection dbname")
}

func main() {
	dbconfigs.RegisterFlags(dbconfigs.App, dbconfigs.AppDebug)
	flag.Parse()

	if *servenv.Version {
		servenv.AppVersion.Print()
		os.Exit(0)
	}

	closer := trace.StartTracing("vtqueryserver")
	defer trace.LogErrorsWhenClosing(closer)

	if len(flag.Args()) > 0 {
		flag.Usage()
		log.Exit("vtqueryserver doesn't take any positional arguments")
	}
	if err := tabletenv.VerifyConfig(); err != nil {
		log.Exitf("invalid config: %v", err)
	}

	tabletenv.Init()

	servenv.Init()

	dbcfgs, err := dbconfigs.Init(*mysqlSocketFile)
	if err != nil {
		log.Fatal(err)
	}
	// DB name must be explicitly set.
	dbcfgs.DBName.Set(dbName)

	err = vtqueryserver.Init(dbcfgs)
	if err != nil {
		log.Exitf("error initializing proxy: %v", err)
	}

	servenv.RunDefault()
}
