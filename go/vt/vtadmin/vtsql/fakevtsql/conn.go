package fakevtsql

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"strings"

	"github.com/stretchr/testify/assert"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/vtadmin/vtadminproto"

	vtadminpb "vitess.io/vitess/go/vt/proto/vtadmin"
)

var (
	// ErrConnClosed is returend when attempting to query a closed connection.
	// It is the identical message to vtsql.ErrConnClosed, but redefined to
	// prevent an import cycle in package vtsql's tests.
	ErrConnClosed = errors.New("use of closed connection")
	// ErrUnrecognizedQuery is returned when QueryCnotext is given a query
	// string the mock is not set up to handle.
	ErrUnrecognizedQuery = errors.New("unrecognized query")
)

type conn struct {
	tablets   []*vtadminpb.Tablet
	shouldErr bool
}

var (
	_ driver.Conn           = (*conn)(nil)
	_ driver.QueryerContext = (*conn)(nil)
)

func (c *conn) Begin() (driver.Tx, error) {
	return nil, nil
}

func (c *conn) Close() error {
	return nil
}

func (c *conn) Prepare(query string) (driver.Stmt, error) {
	return nil, nil
}

func (c *conn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	if c.shouldErr {
		return nil, assert.AnError
	}

	if c == nil {
		return nil, ErrConnClosed
	}

	switch strings.ToLower(query) {
	case "show vitess_tablets", "show tablets":
		columns := []string{"Cell", "Keyspace", "Shard", "TabletType", "ServingState", "Alias", "Hostname", "MasterTermStartTime"}
		vals := [][]interface{}{}

		for _, tablet := range c.tablets {
			vals = append(vals, []interface{}{
				tablet.Tablet.Alias.Cell,
				tablet.Tablet.Keyspace,
				tablet.Tablet.Shard,
				topoproto.TabletTypeLString(tablet.Tablet.Type),
				vtadminproto.TabletServingStateString(tablet.State),
				topoproto.TabletAliasString(tablet.Tablet.Alias),
				tablet.Tablet.Hostname,
				"", // (TODO:@amason) use real values here
			})
		}

		return &rows{
			cols:   columns,
			vals:   vals,
			pos:    0,
			closed: false,
		}, nil
	}

	return nil, fmt.Errorf("%w: %q %v", ErrUnrecognizedQuery, query, args)
}
