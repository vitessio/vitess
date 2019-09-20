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

// Package mysqlproxy is a basic module that proxies a mysql server
// session to appropriate calls in a queryservice back end, with optional
// query normalization.
package mysqlproxy

import (
	"context"
	"fmt"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vttablet/queryservice"

	querypb "vitess.io/vitess/go/vt/proto/query"
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
	case sqlparser.StmtInsert, sqlparser.StmtUpdate, sqlparser.StmtDelete, sqlparser.StmtReplace:
		result, err = mp.executeDML(ctx, session, sql, bindVariables)
	case sqlparser.StmtSelect:
		result, err = mp.executeSelect(ctx, session, sql, bindVariables)
	default:
		result, err = mp.executeOther(ctx, session, sql, bindVariables)
	}

	// N.B. You must return session, even on error. Modeled after vtgate mysql plugin, the
	// vtqueryserver plugin expects you to return a new or updated session and not drop it on the
	// floor during an error.
	return session, result, err
}

// Rollback rolls back the session
func (mp *Proxy) Rollback(ctx context.Context, session *ProxySession) error {
	return mp.doRollback(ctx, session)
}

func (mp *Proxy) doBegin(ctx context.Context, session *ProxySession) error {
	if session.TransactionID != 0 {
		err := mp.doCommit(ctx, session)
		if err != nil {
			return err
		}
	}

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
	vals, _, err := sqlparser.ExtractSetValues(sql)
	if err != nil {
		return nil, err
	}

	for k, v := range vals {
		switch k.Key {
		case "autocommit":
			val, ok := v.(int64)
			if !ok {
				return nil, fmt.Errorf("unexpected value type for autocommit: %T", v)
			}
			switch val {
			case 0:
				session.Autocommit = false
			case 1:
				if !session.Autocommit && session.TransactionID != 0 {
					if err := mp.doCommit(ctx, session); err != nil {
						return nil, err
					}
				}
				session.Autocommit = true
			default:
				return nil, fmt.Errorf("unexpected value for autocommit: %d", val)
			}
		case "charset", "names":
			val, ok := v.(string)
			if !ok {
				return nil, fmt.Errorf("unexpected value type for charset/names: %T", v)
			}
			switch val {
			case "", "utf8", "utf8mb4", "latin1", "default":
				break
			default:
				return nil, fmt.Errorf("unexpected value for charset/names: %v", val)
			}
		default:
			log.Warningf("Ignored inapplicable SET %v = %v", k, v)
		}
	}

	return &sqltypes.Result{}, nil
}

// executeSelect runs the given select statement
func (mp *Proxy) executeSelect(ctx context.Context, session *ProxySession, sql string, bindVariables map[string]*querypb.BindVariable) (*sqltypes.Result, error) {
	if mp.normalize {
		query, comments := sqlparser.SplitMarginComments(sql)
		stmt, err := sqlparser.Parse(query)
		if err != nil {
			return nil, err
		}
		sqlparser.Normalize(stmt, bindVariables, "vtp")
		normalized := sqlparser.String(stmt)
		sql = comments.Leading + normalized + comments.Trailing
	}

	return mp.qs.Execute(ctx, mp.target, sql, bindVariables, session.TransactionID, session.Options)
}

// executeDML runs the given query handling autocommit semantics
func (mp *Proxy) executeDML(ctx context.Context, session *ProxySession, sql string, bindVariables map[string]*querypb.BindVariable) (*sqltypes.Result, error) {
	if mp.normalize {
		query, comments := sqlparser.SplitMarginComments(sql)
		stmt, err := sqlparser.Parse(query)
		if err != nil {
			return nil, err
		}
		sqlparser.Normalize(stmt, bindVariables, "vtp")
		normalized := sqlparser.String(stmt)
		sql = comments.Leading + normalized + comments.Trailing
	}

	if session.TransactionID != 0 {
		return mp.qs.Execute(ctx, mp.target, sql, bindVariables, session.TransactionID, session.Options)

	} else if session.Autocommit {
		queries := []*querypb.BoundQuery{{
			Sql:           sql,
			BindVariables: bindVariables,
		}}

		// This is a stopgap until there is a better way to do autocommit
		results, err := mp.qs.ExecuteBatch(ctx, mp.target, queries, true /* asTransaction */, 0, session.Options)
		if err != nil {
			return nil, err
		}
		return &results[0], nil

	} else {
		result, txnID, err := mp.qs.BeginExecute(ctx, mp.target, sql, bindVariables, session.Options)
		if err != nil {
			return nil, err
		}
		session.TransactionID = txnID
		return result, nil
	}
}

// executeOther runs the given other statement bypassing the normalizer
func (mp *Proxy) executeOther(ctx context.Context, session *ProxySession, sql string, bindVariables map[string]*querypb.BindVariable) (*sqltypes.Result, error) {
	return mp.qs.Execute(ctx, mp.target, sql, bindVariables, session.TransactionID, session.Options)
}
