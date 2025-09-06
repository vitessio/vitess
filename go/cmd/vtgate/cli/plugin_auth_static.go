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

package cli

// This plugin imports staticauthserver to register the flat-file implementation of AuthServer.

import (
	"time"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/vt/utils"
	"vitess.io/vitess/go/vt/vtgate"
)

var (
	mysqlAuthServerStaticFile           string
	mysqlAuthServerStaticString         string
	mysqlAuthServerStaticReloadInterval time.Duration
)

func init() {
	utils.SetFlagStringVar(Main.Flags(), &mysqlAuthServerStaticFile, "mysql-auth-server-static-file", "", "JSON File to read the users/passwords from.")
	utils.SetFlagStringVar(Main.Flags(), &mysqlAuthServerStaticString, "mysql-auth-server-static-string", "", "JSON representation of the users/passwords config.")
	utils.SetFlagDurationVar(Main.Flags(), &mysqlAuthServerStaticReloadInterval, "mysql-auth-static-reload-interval", 0, "Ticker to reload credentials")

	vtgate.RegisterPluginInitializer(func() {
		mysql.InitAuthServerStatic(mysqlAuthServerStaticFile, mysqlAuthServerStaticString, mysqlAuthServerStaticReloadInterval)
	})
}
