package mysqlconn

import (
	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/sqldb"
	"github.com/youtube/vitess/go/sqltypes"

	querypb "github.com/youtube/vitess/go/vt/proto/query"
)

// This file contains the methods needed to implement the sqldb.Conn
// interface.  These methods don't necessarely make sense, but it's
// easier to implement them as is, and then later on refactor
// everything, once the C version of mysql connection is gone.
//
// ExecuteFetch is in query.go.
// Close() is in conn.go.

// ExecuteStreamFetch is part of the sqldb.Conn interface.
// Returns a sqldb.SQLError.
func (c *Conn) ExecuteStreamFetch(query string) error {
	// Sanity check.
	if c.fields != nil {
		return sqldb.NewSQLError(CRCommandsOutOfSync, SSUnknownSQLState, "streaming query already in progress")
	}

	// This is a new command, need to reset the sequence.
	c.sequence = 0

	// Send the query as a COM_QUERY packet.
	if err := c.writeComQuery(query); err != nil {
		return err
	}

	// Get the result.
	_, _, colNumber, err := c.readComQueryResponse()
	if err != nil {
		return err
	}
	if colNumber == 0 {
		// OK packet, means no results. Save an empty Fields array.
		c.fields = make([]*querypb.Field, 0)
		return nil
	}

	// Read the fields, save them.
	fields := make([]querypb.Field, colNumber)
	fieldsPointers := make([]*querypb.Field, colNumber)

	// Read column headers. One packet per column.
	// Build the fields.
	for i := 0; i < colNumber; i++ {
		fieldsPointers[i] = &fields[i]
		if err := c.readColumnDefinition(fieldsPointers[i], i); err != nil {
			return err
		}
	}

	// Read the EOF after the fields if necessary.
	if c.Capabilities&CapabilityClientDeprecateEOF == 0 {
		// EOF is only present here if it's not deprecated.
		data, err := c.readEphemeralPacket()
		if err != nil {
			return sqldb.NewSQLError(CRServerLost, SSUnknownSQLState, "%v", err)
		}
		switch data[0] {
		case EOFPacket:
			// This is what we expect.
			// Warnings and status flags are ignored.
			break
		case ErrPacket:
			// Error packet.
			return parseErrorPacket(data)
		default:
			return sqldb.NewSQLError(CRCommandsOutOfSync, SSUnknownSQLState, "unexpected packet after fields: %v", data)
		}
	}

	c.fields = fieldsPointers
	return nil
}

// Fields is part of the sqldb.Conn interface.
func (c *Conn) Fields() ([]*querypb.Field, error) {
	if c.fields == nil {
		return nil, sqldb.NewSQLError(CRCommandsOutOfSync, SSUnknownSQLState, "no streaming query in progress")
	}
	if len(c.fields) == 0 {
		// The query returned an empty field list.
		return nil, nil
	}
	return c.fields, nil
}

// FetchNext is part of the sqldb.Conn interface.
func (c *Conn) FetchNext() ([]sqltypes.Value, error) {
	if c.fields == nil {
		// We are already done, and the result was closed.
		return nil, sqldb.NewSQLError(CRCommandsOutOfSync, SSUnknownSQLState, "no streaming query in progress")
	}

	if len(c.fields) == 0 {
		// We received no fields, so there is no data.
		return nil, nil
	}

	data, err := c.ReadPacket()
	if err != nil {
		return nil, err
	}

	switch data[0] {
	case EOFPacket:
		// This packet may be one of two kinds:
		// - an EOF packet,
		// - an OK packet with an EOF header if
		// CapabilityClientDeprecateEOF is set.
		// We do not parse it anyway, so it doesn't matter.

		// Warnings and status flags are ignored.
		c.fields = nil
		return nil, nil
	case ErrPacket:
		// Error packet.
		return nil, parseErrorPacket(data)
	}

	// Regular row.
	return c.parseRow(data, c.fields)
}

// CloseResult is part of the sqldb.Conn interface.
// Just drain the remaining values.
func (c *Conn) CloseResult() {
	for c.fields != nil {
		rows, err := c.FetchNext()
		if err != nil || rows == nil {
			// We either got an error, or got the last result.
			c.fields = nil
		}
	}
}

// IsClosed is part of the sqldb.Conn interface.
func (c *Conn) IsClosed() bool {
	return c.ConnectionID == 0
}

// Shutdown is part of the sqldb.Conn interface.
func (c *Conn) Shutdown() {
	c.ConnectionID = 0
	c.conn.Close()
}

// ID is part of the sqldb.Conn interface.
func (c *Conn) ID() int64 {
	return int64(c.ConnectionID)
}

func init() {
	sqldb.Register("mysqlconn", func(params sqldb.ConnParams) (sqldb.Conn, error) {
		ctx := context.Background()
		return Connect(ctx, &params)
	})

	// Uncomment this and comment out the call to sqldb.RegisterDefault in
	// go/mysql/mysql.go to make this the default.

	//	sqldb.RegisterDefault(func(params sqldb.ConnParams) (sqldb.Conn, error) {
	//		ctx := context.Background()
	//		return Connect(ctx, &params)
	//	})
}
