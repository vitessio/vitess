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
	"vitess.io/vitess/go/vt/vtgate"
)

var (
	mysqlAuthServerStaticFile           string
	mysqlAuthServerStaticString         string
	mysqlAuthServerStaticReloadInterval time.Duration
)

func init() {
	Main.Flags().StringVar(&mysqlAuthServerStaticFile, "mysql_auth_server_static_file", "", "JSON File to read the users/passwords from.")
	Main.Flags().StringVar(&mysqlAuthServerStaticString, "mysql_auth_server_static_string", "", "JSON representation of the users/passwords config.")
	Main.Flags().DurationVar(&mysqlAuthServerStaticReloadInterval, "mysql_auth_static_reload_interval", 0, "Ticker to reload credentials")

	vtgate.RegisterPluginInitializer(func() {
		mysql.InitAuthServerStatic(mysqlAuthServerStaticFile, mysqlAuthServerStaticString, mysqlAuthServerStaticReloadInterval)
	})
}
