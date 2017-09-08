// +build go1.8

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

// TODO(sougou): Merge this with driver.go once go1.7 is deprecated.
// Also write tests for these new functions once go1.8 becomes mainstream.

package vitessdriver

import (
	"context"
	"database/sql/driver"
	"errors"
)

var (
	errNoIntermixing        = errors.New("named and positional arguments intermixing disallowed")
	errIsolationUnsupported = errors.New("isolation levels are not supported")
)

// Type-check interfaces.
var (
	_ driver.QueryerContext   = &conn{}
	_ driver.ExecerContext    = &conn{}
	_ driver.StmtQueryContext = &stmt{}
	_ driver.StmtExecContext  = &stmt{}
)

func (c *conn) BeginTx(_ context.Context, opts driver.TxOptions) (driver.Tx, error) {
	// We don't use the context. The function signature accepts the context
	// to signal to the driver that it's allowed to call Rollback on Cancel.
	if opts.Isolation != driver.IsolationLevel(0) || opts.ReadOnly {
		return nil, errIsolationUnsupported
	}
	return c.Begin()
}

func (c *conn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	if c.Streaming {
		return nil, errors.New("Exec not allowed for streaming connections")
	}

	bv, err := c.convert.bindVarsFromNamedValues(args)
	if err != nil {
		return nil, err
	}
	qr, err := c.session.Execute(ctx, query, bv)
	if err != nil {
		return nil, err
	}
	return result{int64(qr.InsertID), int64(qr.RowsAffected)}, nil
}

func (c *conn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	bv, err := c.convert.bindVarsFromNamedValues(args)
	if err != nil {
		return nil, err
	}

	if c.Streaming {
		stream, err := c.session.StreamExecute(ctx, query, bv)
		if err != nil {
			return nil, err
		}
		return newStreamingRows(stream, nil, c.convert), nil
	}

	qr, err := c.session.Execute(ctx, query, bv)
	if err != nil {
		return nil, err
	}
	return newRows(qr, c.convert), nil
}

func (s *stmt) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	return s.c.ExecContext(ctx, s.query, args)
}

func (s *stmt) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	return s.c.QueryContext(ctx, s.query, args)
}
