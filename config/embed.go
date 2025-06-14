package config

import _ "embed"

//go:embed init_db.sql
var DefaultInitDB string

//go:embed mycnf/default.cnf
var MycnfDefault string

//go:embed mycnf/mysql80.cnf
var MycnfMySQL80 string

//go:embed mycnf/mysql8026.cnf
var MycnfMySQL8026 string

//go:embed mycnf/mysql84.cnf
var MycnfMySQL84 string
