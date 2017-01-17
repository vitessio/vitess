package mysqlconn

import (
	"bufio"
	"fmt"
	"io"
	"net"

	"github.com/youtube/vitess/go/sqldb"

	querypb "github.com/youtube/vitess/go/vt/proto/query"
)

const (
	// connBufferSize is how much we buffer for reading and
	// writing.
	connBufferSize = 16 * 1024
)

// Conn is a connection between a client and a server, using the MySQL
// binary protocol. It is built on top of an existing net.Conn, that
// has already been established.
//
// Use Connect on the client side to create a connection.
// Use NewListener to create a server side and listen for connections.
type Conn struct {
	// conn is the underlying network connection.
	// Calling Close() on the Conn will close this connection.
	// If there are any ongoing reads or writes, they may get interrupted.
	conn net.Conn

	// ConnectionID is set:
	// - at Connect() time for clients, with the value returned by
	// the server.
	// - at accept time for the server.
	// If Close() or Shutdown() was called, this is reset to 0.
	ConnectionID uint32

	// Capabilities is the current set of features this connection
	// is using.  It is the features that are both supported by
	// the client and the server, and currently in use.
	// It is set after the initial handshake.
	//
	// It is only used for CapabilityClientDeprecateEOF.
	Capabilities uint32

	// CharacterSet is the character set used by the other side of the
	// connection.
	// It is set during the initial handshake.
	// See the values in constants.go.
	CharacterSet uint8

	// SchemaName is the default database name to use. It is set
	// during handshake, and by ComInitDb packets. Both client and
	// servers maintain it.
	SchemaName string

	// ServerVersion is set during Connect with the server
	// version.  It is not changed afterwards. It is unused for
	// server-side connections.
	ServerVersion string

	// StatusFlags are the status flags we will base our returned flags on.
	// This is a bit field, with values documented in constants.go.
	// An interesting value here would be ServerStatusAutocommit.
	// It is only used by the server. These flags can be changed
	// by Handler methods.
	StatusFlags uint16

	// ClientData is a place where an application can store any
	// connection-related data. Mostly used on the server side, to
	// avoid maps indexed by ConnectionID for instance.
	ClientData interface{}

	// Packet encoding variables.
	reader   *bufio.Reader
	writer   *bufio.Writer
	sequence uint8

	// Internal variables for sqldb.Conn API for stream queries.
	// This is set only if a streaming query is in progress.
	fields []*querypb.Field
}

func newConn(conn net.Conn) *Conn {
	return &Conn{
		conn: conn,

		reader:   bufio.NewReaderSize(conn, connBufferSize),
		writer:   bufio.NewWriterSize(conn, connBufferSize),
		sequence: 0,
	}
}

func (c *Conn) readOnePacket() ([]byte, error) {
	var header [4]byte

	if _, err := io.ReadFull(c.reader, header[:]); err != nil {
		return nil, fmt.Errorf("io.ReadFull(header size) failed: %v", err)
	}

	sequence := uint8(header[3])
	if sequence != c.sequence {
		return nil, fmt.Errorf("invalid sequence, expected %v got %v", c.sequence, sequence)
	}

	c.sequence++

	length := int(uint32(header[0]) | uint32(header[1])<<8 | uint32(header[2])<<16)
	if length == 0 {
		// This can be caused by the packet after a packet of
		// exactly size MaxPacketSize.
		return nil, nil
	}

	data := make([]byte, length)
	if _, err := io.ReadFull(c.reader, data); err != nil {
		return nil, fmt.Errorf("io.ReadFull(packet body of length %v) failed: %v", length, err)
	}
	return data, nil
}

// ReadPacket reads a packet from the underlying connection.
// It re-assembles packets that span more than one message.
func (c *Conn) ReadPacket() ([]byte, error) {
	// Optimize for a single packet case.
	data, err := c.readOnePacket()
	if err != nil {
		return nil, err
	}

	// This is a single packet.
	if len(data) < MaxPacketSize {
		return data, nil
	}

	// There is more than one packet, read them all.
	for {
		next, err := c.readOnePacket()
		if err != nil {
			return nil, err
		}

		if len(next) == 0 {
			// Again, the packet after a packet of exactly size MaxPacketSize.
			break
		}

		data = append(data, next...)
		if len(next) < MaxPacketSize {
			break
		}
	}

	return data, nil
}

// writePacket writes a packet, possibly cutting it into multiple
// chunks.  Note this is not very efficient, as the client probably
// has to build the []byte and that makes a memory copy.
//
// TODO(alainjobart): to write packets more efficiently, the following API should be added:
// startPacket(length int)
//   - remembers we started writing a packet.
//   - writes the first packet header.
// writePacketChunk(data []byte) / writeByte(b byte)
//   - writes the bytes, respecting packet boundaries.
//   - if we need to go from one packet to the next, do it.
// - checks we're not past the initial packet size.
// finishPacket()
//   - checks the packet is done.
//   - write an empty packet if the write was a multiple of MaxPacketSize
func (c *Conn) writePacket(data []byte) error {
	index := 0
	length := len(data)

	for {
		// Packet length is capped to MaxPacketSize.
		packetLength := length
		if packetLength > MaxPacketSize {
			packetLength = MaxPacketSize
		}

		// Compute and write the header.
		var header [4]byte
		header[0] = byte(packetLength)
		header[1] = byte(packetLength >> 8)
		header[2] = byte(packetLength >> 16)
		header[3] = c.sequence
		if n, err := c.writer.Write(header[:]); err != nil {
			return fmt.Errorf("Write(header) failed: %v", err)
		} else if n != 4 {
			return fmt.Errorf("Write(header) returned a short write: %v < 4", n)
		}

		// Write the body.
		if n, err := c.writer.Write(data[index : index+packetLength]); err != nil {
			return fmt.Errorf("Write(packet) failed: %v", err)
		} else if n != packetLength {
			return fmt.Errorf("Write(packet) returned a short write: %v < %v", n, packetLength)
		}

		// Update our state.
		c.sequence++
		length -= packetLength
		if length == 0 {
			if packetLength == MaxPacketSize {
				// The packet we just sent had exactly
				// MaxPacketSize size, we need to
				// sent a zero-size packet too.
				header[0] = 0
				header[1] = 0
				header[2] = 0
				header[3] = c.sequence
				if n, err := c.writer.Write(header[:]); err != nil {
					return fmt.Errorf("Write(empty header) failed: %v", err)
				} else if n != 4 {
					return fmt.Errorf("Write(empty header) returned a short write: %v < 4", n)
				}
				c.sequence++
			}
			return nil
		}
		index += packetLength
	}
}

func (c *Conn) flush() {
	c.writer.Flush()
}

// Close closes the connection.
func (c *Conn) Close() {
	c.ConnectionID = 0
	c.conn.Close()
}

//
// Packet writing methods, for generic packets.
//

func (c *Conn) writeOKPacket(affectedRows, lastInsertID uint64, flags uint16, warnings uint16) error {
	length := 1 + // OKPacket
		lenEncIntSize(affectedRows) +
		lenEncIntSize(lastInsertID) +
		2 + // flags
		2 // warnings
	data := make([]byte, length)
	pos := 0
	pos = writeByte(data, pos, OKPacket)
	pos = writeLenEncInt(data, pos, affectedRows)
	pos = writeLenEncInt(data, pos, lastInsertID)
	pos = writeUint16(data, pos, flags)
	pos = writeUint16(data, pos, warnings)

	if err := c.writePacket(data); err != nil {
		return err
	}
	c.flush()
	return nil
}

// writeOKPacketWithEOFHeader writes an OK packet with an EOF header.
// This is used at the end of a result set if
// CapabilityClientDeprecateEOF is set.
func (c *Conn) writeOKPacketWithEOFHeader(affectedRows, lastInsertID uint64, flags uint16, warnings uint16) error {
	length := 1 + // EOFPacket
		lenEncIntSize(affectedRows) +
		lenEncIntSize(lastInsertID) +
		2 + // flags
		2 // warnings
	data := make([]byte, length)
	pos := 0
	pos = writeByte(data, pos, EOFPacket)
	pos = writeLenEncInt(data, pos, affectedRows)
	pos = writeLenEncInt(data, pos, lastInsertID)
	pos = writeUint16(data, pos, flags)
	pos = writeUint16(data, pos, warnings)

	if err := c.writePacket(data); err != nil {
		return err
	}
	c.flush()
	return nil
}

func (c *Conn) writeErrorPacket(errorCode uint16, sqlState string, format string, args ...interface{}) error {
	errorMessage := fmt.Sprintf(format, args...)
	length := 1 + 2 + 1 + 5 + len(errorMessage)
	data := make([]byte, length)
	pos := 0
	pos = writeByte(data, pos, ErrPacket)
	pos = writeUint16(data, pos, errorCode)
	pos = writeByte(data, pos, '#')
	if sqlState == "" {
		sqlState = SSSignalException
	}
	if len(sqlState) != 5 {
		panic("sqlState has to be 5 characters long")
	}
	pos = writeEOFString(data, pos, sqlState)
	pos = writeEOFString(data, pos, errorMessage)

	if err := c.writePacket(data); err != nil {
		return err
	}
	c.flush()
	return nil
}

func (c *Conn) writeErrorPacketFromError(err error) error {
	if se, ok := err.(*sqldb.SQLError); ok {
		return c.writeErrorPacket(uint16(se.Num), se.State, "%v", se.Message)
	}

	return c.writeErrorPacket(ERUnknownError, SSSignalException, "unknown error: %v", err)
}

func (c *Conn) writeEOFPacket(flags uint16, warnings uint16) error {
	length := 5
	data := make([]byte, length)
	pos := 0
	pos = writeByte(data, pos, EOFPacket)
	pos = writeUint16(data, pos, warnings)
	pos = writeUint16(data, pos, flags)

	if err := c.writePacket(data); err != nil {
		return err
	}
	return nil
}

//
// Packet parsing methods, for generic packets.
//

func parseOKPacket(data []byte) (uint64, uint64, uint16, uint16, error) {
	// We already read the type.
	pos := 1

	// Affected rows.
	affectedRows, pos, ok := readLenEncInt(data, pos)
	if !ok {
		return 0, 0, 0, 0, fmt.Errorf("invalid OK packet affectedRows: %v", data)
	}

	// Last Insert ID.
	lastInsertID, pos, ok := readLenEncInt(data, pos)
	if !ok {
		return 0, 0, 0, 0, fmt.Errorf("invalid OK packet lastInsertID: %v", data)
	}

	// Status flags.
	statusFlags, pos, ok := readUint16(data, pos)
	if !ok {
		return 0, 0, 0, 0, fmt.Errorf("invalid OK packet statusFlags: %v", data)
	}

	// Warnings.
	warnings, pos, ok := readUint16(data, pos)
	if !ok {
		return 0, 0, 0, 0, fmt.Errorf("invalid OK packet warnings: %v", data)
	}

	return affectedRows, lastInsertID, statusFlags, warnings, nil
}

func parseErrorPacket(data []byte) error {
	// We already read the type.
	pos := 1

	// Error code is 2 bytes.
	code, pos, ok := readUint16(data, pos)
	if !ok {
		return fmt.Errorf("invalid error packet code: %v", data)
	}

	// '#' marker of the SQL state is 1 byte. Ignored.
	pos++

	// SQL state is 5 bytes
	sqlState, pos, ok := readBytes(data, pos, 5)
	if !ok {
		return fmt.Errorf("invalid error packet sqlState: %v", data)
	}

	// Human readable error message is the rest.
	msg := string(data[pos:])

	return sqldb.NewSQLError(int(code), string(sqlState), "%v", msg)
}
