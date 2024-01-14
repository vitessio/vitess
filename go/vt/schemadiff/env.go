package schemadiff

import (
	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/vt/sqlparser"
)

type Environment struct {
	CollationEnv *collations.Environment
	DefaultColl  collations.ID
	Parser       *sqlparser.Parser
}

func NewTestEnv() *Environment {
	return &Environment{
		CollationEnv: collations.MySQL8(),
		DefaultColl:  collations.MySQL8().DefaultConnectionCharset(),
		Parser:       sqlparser.NewTestParser(),
	}
}

func NewEnv(collEnv *collations.Environment, defaultColl collations.ID, parser *sqlparser.Parser) *Environment {
	return &Environment{
		CollationEnv: collEnv,
		DefaultColl:  defaultColl,
		Parser:       parser,
	}
}
