package config

import _ "embed"

//go:embed init_db.sql
var DefaultInitDB string

//go:embed mycnf/default.cnf
var MycnfDefault string

//go:embed mycnf/mariadb100.cnf
var MycnfMariaDB100 string

//go:embed mycnf/mariadb101.cnf
var MycnfMariaDB101 string

//go:embed mycnf/mariadb102.cnf
var MycnfMariaDB102 string

//go:embed mycnf/mariadb103.cnf
var MycnfMariaDB103 string

//go:embed mycnf/mariadb104.cnf
var MycnfMariaDB104 string

//go:embed mycnf/mysql57.cnf
var MycnfMySQL57 string

//go:embed mycnf/mysql80.cnf
var MycnfMySQL80 string
