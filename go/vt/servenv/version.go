package servenv

import "flag"

const versionName = "13.0.0-rc1"

// MySQLServerVersion is what Vitess will present as it's version during the connection handshake,
// and as the value to the @@version system variable. If nothing is provided, Vitess will report itself as
// a specific MySQL version with the vitess version appended to it
var MySQLServerVersion = flag.String("mysql_server_version", "", "MySQL server version to advertise.")
