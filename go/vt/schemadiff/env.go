package schemadiff

import (
	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/mysql/config"
	"vitess.io/vitess/go/vt/sqlparser"
)

type Environment struct {
	CollationEnv *collations.Environment
	DefaultColl  collations.ID
	Parser       *sqlparser.Parser
	MySQLVersion string
}

func NewTestEnv() *Environment {
	return &Environment{
		CollationEnv: collations.MySQL8(),
		DefaultColl:  collations.MySQL8().DefaultConnectionCharset(),
		Parser:       sqlparser.NewTestParser(),
		MySQLVersion: config.DefaultMySQLVersion,
	}
}

func NewEnv(collEnv *collations.Environment, defaultColl collations.ID, parser *sqlparser.Parser, mysqlVersion string) *Environment {
	return &Environment{
		CollationEnv: collEnv,
		DefaultColl:  defaultColl,
		Parser:       parser,
		MySQLVersion: mysqlVersion,
	}
}
