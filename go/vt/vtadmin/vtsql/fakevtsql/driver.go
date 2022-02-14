package fakevtsql

import (
	"context"
	"database/sql/driver"

	vtadminpb "vitess.io/vitess/go/vt/proto/vtadmin"
)

type fakedriver struct {
	tablets   []*vtadminpb.Tablet
	shouldErr bool
}

var _ driver.Driver = (*fakedriver)(nil)

func (d *fakedriver) Open(name string) (driver.Conn, error) {
	return &conn{tablets: d.tablets, shouldErr: d.shouldErr}, nil
}

// Connector implements the driver.Connector interface, providing a sql-like
// thing that can respond to vtadmin vtsql queries with mocked data.
type Connector struct {
	Tablets []*vtadminpb.Tablet
	// (TODO:@amason) - allow distinction between Query errors and errors on
	// Rows operations (e.g. Next, Err, Scan).
	ShouldErr bool
}

var _ driver.Connector = (*Connector)(nil)

// Connect is part of the driver.Connector interface.
func (c *Connector) Connect(ctx context.Context) (driver.Conn, error) {
	return &conn{tablets: c.Tablets, shouldErr: c.ShouldErr}, nil
}

// Driver is part of the driver.Connector interface.
func (c *Connector) Driver() driver.Driver {
	return &fakedriver{tablets: c.Tablets, shouldErr: c.ShouldErr}
}
