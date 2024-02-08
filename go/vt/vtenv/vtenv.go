/*
Copyright 2024 The Vitess Authors.

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

package vtenv

import (
	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/mysql/config"
	"vitess.io/vitess/go/vt/sqlparser"
)

type Environment struct {
	collationEnv   *collations.Environment
	parser         *sqlparser.Parser
	mysqlVersion   string
	truncateUILen  int
	truncateErrLen int
}

type Options struct {
	MySQLServerVersion string
	TruncateUILen      int
	TruncateErrLen     int
}

func New(cfg Options) (*Environment, error) {
	if cfg.MySQLServerVersion == "" {
		cfg.MySQLServerVersion = config.DefaultMySQLVersion
	}
	parser, err := sqlparser.New(sqlparser.Options{
		MySQLServerVersion: cfg.MySQLServerVersion,
		TruncateErrLen:     cfg.TruncateErrLen,
		TruncateUILen:      cfg.TruncateUILen,
	})
	if err != nil {
		return nil, err
	}
	return &Environment{
		collationEnv:   collations.NewEnvironment(cfg.MySQLServerVersion),
		parser:         parser,
		mysqlVersion:   cfg.MySQLServerVersion,
		truncateUILen:  cfg.TruncateUILen,
		truncateErrLen: cfg.TruncateErrLen,
	}, nil
}

func NewTestEnv() *Environment {
	return &Environment{
		collationEnv:   collations.NewEnvironment(config.DefaultMySQLVersion),
		parser:         sqlparser.NewTestParser(),
		mysqlVersion:   config.DefaultMySQLVersion,
		truncateUILen:  512,
		truncateErrLen: 0,
	}
}

func (e *Environment) CollationEnv() *collations.Environment {
	return e.collationEnv
}

func (e *Environment) Parser() *sqlparser.Parser {
	return e.parser
}

func (e *Environment) MySQLVersion() string {
	return e.mysqlVersion
}

// TruncateForUI is used when displaying queries on various Vitess status pages
// to keep the pages small enough to load and render properly
func (e *Environment) TruncateForUI(query string) string {
	return sqlparser.TruncateQuery(query, e.truncateUILen)
}

// TruncateForLog is used when displaying queries as part of error logs
// to avoid overwhelming logging systems with potentially long queries and
// bind value data.
func (e *Environment) TruncateForLog(query string) string {
	return sqlparser.TruncateQuery(query, e.truncateErrLen)
}

func (e *Environment) TruncateErrLen() int {
	return e.truncateErrLen
}
