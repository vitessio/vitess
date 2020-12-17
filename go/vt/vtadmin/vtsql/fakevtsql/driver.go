/*
Copyright 2020 The Vitess Authors.

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
