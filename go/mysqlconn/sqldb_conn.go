package mysqlconn

import (
	"fmt"

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
func (c *Conn) ExecuteStreamFetch(query string) error {
	// Sanity check.
	if c.fields != nil {
		return fmt.Errorf("streaming query already in progress")
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
		data, err := c.readPacket()
		if err != nil {
			return err
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
			return fmt.Errorf("unexpected packet after fields: %v", data)
		}
	}

	c.fields = fieldsPointers
	return nil
}

// Fields is part of the sqldb.Conn interface.
func (c *Conn) Fields() ([]*querypb.Field, error) {
	if c.fields == nil {
		return nil, fmt.Errorf("no streaming query in progress")
	}
	return c.fields, nil
}

// FetchNext is part of the sqldb.Conn interface.
func (c *Conn) FetchNext() ([]sqltypes.Value, error) {
	if c.fields == nil {
		// We are already done.
		return nil, nil
	}

	data, err := c.readPacket()
	if err != nil {
		return nil, err
	}

	switch data[0] {
	case OKPacket:
		// The entire contents of the packet is ignored.
		// This packet is only seen if CapabilityClientDeprecateEOF is set.
		// But we can still understand it anyway.
		c.fields = nil
		return nil, nil
	case EOFPacket:
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
		_, err := c.FetchNext()
		if err != nil {
			c.fields = nil
			return
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

// ReadPacket is part of the sqldb.Conn interface.
func (c *Conn) ReadPacket() ([]byte, error) {
	return c.readPacket()
}

// SendCommand is part of the sqldb.Conn interface.
// Note this implementation is not efficient (and the command type is
// a byte, not a uint32), but this is not used often.  We'll refactor it.
func (c *Conn) SendCommand(command uint32, data []byte) error {
	// This is a new command, need to reset the sequence.
	c.sequence = 0

	fullData := make([]byte, len(data)+1)
	fullData[0] = byte(command)
	copy(fullData[1:], data)
	return c.writePacket(fullData)
}

func init() {
	sqldb.Register("sqlconn", func(params sqldb.ConnParams) (sqldb.Conn, error) {
		ctx := context.Background()
		return Connect(ctx, &params)
	})
}
