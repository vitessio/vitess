/*
Copyright 2022 The Vitess Authors.

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

package servenv

import (
	"fmt"

	"github.com/spf13/pflag"

	"vitess.io/vitess/go/mysql/config"
)

// mySQLServerVersion is what Vitess will present as it's version during the connection handshake,
// and as the value to the @@version system variable. If nothing is provided, Vitess will report itself as
// a specific MySQL version with the vitess version appended to it
var mySQLServerVersion = fmt.Sprintf("%s-Vitess", config.DefaultMySQLVersion)

// RegisterMySQLServerFlags installs the flags needed to specify or expose a
// particular MySQL server version from Vitess.
func RegisterMySQLServerFlags(fs *pflag.FlagSet) {
	fs.StringVar(&mySQLServerVersion, "mysql_server_version", mySQLServerVersion, "MySQL server version to advertise.")
}

// MySQLServerVersion returns the value of the `--mysql_server_version` flag.
func MySQLServerVersion() string {
	return mySQLServerVersion
}

func init() {
	for _, cmd := range []string{
		"mysqlctl",
		"mysqlctld",
		"vtbackup",
		"vtcombo",
		"vtctl",
		"vtctld",
		"vtctldclient",
		"vtexplain",
		"vtgate",
		"vtgateclienttest",
		"vttablet",
		"vttestserver",
	} {
		OnParseFor(cmd, RegisterMySQLServerFlags)
	}
}
