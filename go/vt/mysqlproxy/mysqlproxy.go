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

// Package mysqlproxy is a basic module that proxies a mysql server
// session to appropriate calls in a queryservice back end, with optional
// query normalization.
package mysqlproxy

import (
	"context"
	"fmt"

	log "github.com/golang/glog"

	"github.com/youtube/vitess/go/sqltypes"
	"github.com/youtube/vitess/go/vt/sqlparser"
	"github.com/youtube/vitess/go/vt/vttablet/queryservice"

	querypb "github.com/youtube/vitess/go/vt/proto/query"
)

// ProxySession holds session state for the proxy
type ProxySession struct {
	TransactionID int64
	TargetString  string
	Options       *querypb.ExecuteOptions
	Autocommit    bool
}

// Proxy wraps the standalone query service
type Proxy struct {
	target    *querypb.Target
	qs        queryservice.QueryService
	normalize bool
}

// NewProxy creates a new proxy
func NewProxy(target *querypb.Target, qs queryservice.QueryService, normalize bool) *Proxy {
	return &Proxy{
		target:    target,
		qs:        qs,
		normalize: normalize,
	}
}

// Execute runs the given sql query in the specified session
func (mp *Proxy) Execute(ctx context.Context, session *ProxySession, sql string, bindVariables map[string]*querypb.BindVariable) (*ProxySession, *sqltypes.Result, error) {
	var err error
	result := &sqltypes.Result{}

	switch sqlparser.Preview(sql) {
	case sqlparser.StmtBegin:
		err = mp.doBegin(ctx, session)
	case sqlparser.StmtCommit:
		err = mp.doCommit(ctx, session)
	case sqlparser.StmtRollback:
		err = mp.doRollback(ctx, session)
	case sqlparser.StmtSet:
		result, err = mp.doSet(ctx, session, sql, bindVariables)
	default:
		result, err = mp.doExecute(ctx, session, sql, bindVariables)
	}

	if err != nil {
		return nil, nil, err
	}

	return session, result, nil
}

// Rollback rolls back the session
func (mp *Proxy) Rollback(ctx context.Context, session *ProxySession) error {
	return mp.doRollback(ctx, session)
}

func (mp *Proxy) doBegin(ctx context.Context, session *ProxySession) error {
	txID, err := mp.qs.Begin(ctx, mp.target, session.Options)
	if err != nil {
		return err
	}
	session.TransactionID = txID
	return nil
}

func (mp *Proxy) doCommit(ctx context.Context, session *ProxySession) error {
	if session.TransactionID == 0 {
		return fmt.Errorf("commit: no open transaction")

	}
	err := mp.qs.Commit(ctx, mp.target, session.TransactionID)
	session.TransactionID = 0
	return err
}

// Rollback rolls back the session
func (mp *Proxy) doRollback(ctx context.Context, session *ProxySession) error {
	if session.TransactionID != 0 {
		err := mp.qs.Rollback(ctx, mp.target, session.TransactionID)
		session.TransactionID = 0
		return err
	}
	return nil
}

// Set is currently ignored
func (mp *Proxy) doSet(ctx context.Context, session *ProxySession, sql string, bindVariables map[string]*querypb.BindVariable) (*sqltypes.Result, error) {
	vals, charset, err := sqlparser.ExtractSetValues(sql)
	if err != nil {
		return nil, err
	}
	if len(vals) > 0 && charset != "" {
		return nil, err
	}

	switch charset {
	case "", "utf8", "utf8mb4", "latin1", "default":
		break
	default:
		return nil, fmt.Errorf("unexpected value for charset: %v", charset)
	}

	for k, v := range vals {
		log.Warningf("Ignored inapplicable SET %v = %v", k, v)
	}

	return &sqltypes.Result{}, nil
}

// doExecute runs the given query
func (mp *Proxy) doExecute(ctx context.Context, session *ProxySession, sql string, bindVariables map[string]*querypb.BindVariable) (*sqltypes.Result, error) {
	if mp.normalize {
		query, comments := sqlparser.SplitTrailingComments(sql)
		stmt, err := sqlparser.Parse(query)
		if err != nil {
			return nil, err
		}
		sqlparser.Normalize(stmt, bindVariables, "vtp")
		normalized := sqlparser.String(stmt)
		sql = normalized + comments
	}

	return mp.qs.Execute(ctx, mp.target, sql, bindVariables, session.TransactionID, session.Options)
}
