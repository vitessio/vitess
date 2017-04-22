// Copyright 2016, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// +build go1.8

// TODO(sougou): Merge this with driver.go once go1.7 is deprecated.
// Also write tests for these new functions once go1.8 becomes mainstream.

package vitessdriver

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
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
	// to singal to the driver that it's allowed to call Rollback on Cancel.
	if opts.Isolation != driver.IsolationLevel(0) || opts.ReadOnly {
		return nil, errIsolationUnsupported
	}
	return c.Begin()
}

func (c *conn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	if c.Streaming {
		return nil, errors.New("Exec not allowed for streaming connections")
	}

	bv, err := bindVarsFromNamedValues(args)
	if err != nil {
		return nil, err
	}
	qr, err := c.vsn.Execute(ctx, query, bv)
	if err != nil {
		return nil, err
	}
	return result{int64(qr.InsertID), int64(qr.RowsAffected)}, nil
}

func (c *conn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	bv, err := bindVarsFromNamedValues(args)
	if err != nil {
		return nil, err
	}

	if c.Streaming {
		stream, err := c.vsn.StreamExecute(ctx, query, bv)
		if err != nil {
			return nil, err
		}
		return newStreamingRows(stream, nil), nil
	}

	qr, err := c.vsn.Execute(ctx, query, bv)
	if err != nil {
		return nil, err
	}
	return newRows(qr), nil
}

func (s *stmt) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	return s.c.ExecContext(ctx, s.query, args)
}

func (s *stmt) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	return s.c.QueryContext(ctx, s.query, args)
}

func bindVarsFromNamedValues(args []driver.NamedValue) (map[string]interface{}, error) {
	bv := make(map[string]interface{}, len(args))
	nameUsed := false
	for i, v := range args {
		if i == 0 {
			// Determine if args are based on names or ordinals.
			if v.Name != "" {
				nameUsed = true
			}
		} else {
			// Verify that there's no intermixing.
			if nameUsed && v.Name == "" {
				return nil, errNoIntermixing
			}
			if !nameUsed && v.Name != "" {
				return nil, errNoIntermixing
			}
		}
		if v.Name == "" {
			bv[fmt.Sprintf("v%d", i+1)] = v.Value
		} else {
			if v.Name[0] == ':' || v.Name[0] == '@' {
				bv[v.Name[1:]] = v.Value
			} else {
				bv[v.Name] = v.Value
			}
		}
	}
	return bv, nil
}
