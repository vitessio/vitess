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

package mysql

import (
	"bufio"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"time"

	"vitess.io/vitess/go/bucketpool"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/sync2"
	"vitess.io/vitess/go/vt/log"
	querypb "vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
)

const (
	// connBufferSize is how much we buffer for reading and
	// writing. It is also how much we allocate for ephemeral buffers.
	connBufferSize = 16 * 1024
)

// Constants for how ephemeral buffers were used for reading / writing.
const (
	// ephemeralUnused means the ephemeral buffer is not in use at this
	// moment. This is the default value, and is checked so we don't
	// read or write a packet while one is already used.
	ephemeralUnused = iota

	// ephemeralWrite means we currently in process of writing from  currentEphemeralBuffer
	ephemeralWrite

	// ephemeralRead means we currently in process of reading into currentEphemeralBuffer
	ephemeralRead
)

// A Getter has a Get()
type Getter interface {
	Get() *querypb.VTGateCallerID
}

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

	// For server-side connections, listener points to the server object.
	listener *Listener

	// ConnectionID is set:
	// - at Connect() time for clients, with the value returned by
	// the server.
	// - at accept time for the server.
	ConnectionID uint32

	// closed is set to true when Close() is called on the connection.
	closed sync2.AtomicBool

	// Capabilities is the current set of features this connection
	// is using.  It is the features that are both supported by
	// the client and the server, and currently in use.
	// It is set during the initial handshake.
	//
	// It is only used for CapabilityClientDeprecateEOF
	// and CapabilityClientFoundRows.
	Capabilities uint32

	// CharacterSet is the character set used by the other side of the
	// connection.
	// It is set during the initial handshake.
	// See the values in constants.go.
	CharacterSet uint8

	// User is the name used by the client to connect.
	// It is set during the initial handshake.
	User string

	// UserData is custom data returned by the AuthServer module.
	// It is set during the initial handshake.
	UserData Getter

	// SchemaName is the default database name to use. It is set
	// during handshake, and by ComInitDb packets. Both client and
	// servers maintain it.
	SchemaName string

	// ServerVersion is set during Connect with the server
	// version.  It is not changed afterwards. It is unused for
	// server-side connections.
	ServerVersion string

	// flavor contains the auto-detected flavor for this client
	// connection. It is unused for server-side connections.
	flavor flavor

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
	bufferedReader *bufio.Reader
	bufferedWriter *bufio.Writer
	sequence       uint8

	// fields contains the fields definitions for an on-going
	// streaming query. It is set by ExecuteStreamFetch, and
	// cleared by the last FetchNext().  It is nil if no streaming
	// query is in progress.  If the streaming query returned no
	// fields, this is set to an empty array (but not nil).
	fields []*querypb.Field

	// Keep track of how and of the buffer we allocated for an
	// ephemeral packet on the read and write sides.
	// These fields are used by:
	// - startEphemeralPacket / writeEphemeralPacket methods for writes.
	// - readEphemeralPacket / recycleReadPacket methods for reads.
	currentEphemeralPolicy int
	// currentEphemeralBuffer for tracking allocated temporary buffer for writes and reads respectively.
	// It can be allocated from bufPool or heap and should be recycled in the same manner.
	currentEphemeralBuffer *[]byte

	// StatementID is the prepared statement ID.
	StatementID uint32

	// PrepareData is the map to use a prepared statement.
	PrepareData map[uint32]*PrepareData
}

// PrepareData is a buffer used for store prepare statement meta data
type PrepareData struct {
	StatementID uint32
	PrepareStmt string
	ParamsCount uint16
	ParamsType  []int32
	ColumnNames []string
	BindVars    map[string]*querypb.BindVariable
}

// bufPool is used to allocate and free buffers in an efficient way.
var bufPool = bucketpool.New(connBufferSize, MaxPacketSize)

// writersPool is used for pooling bufio.Writer objects.
var writersPool = sync.Pool{New: func() interface{} { return bufio.NewWriterSize(nil, connBufferSize) }}

// newConn is an internal method to create a Conn. Used by client and server
// side for common creation code.
func newConn(conn net.Conn) *Conn {
	return &Conn{
		conn:           conn,
		closed:         sync2.NewAtomicBool(false),
		bufferedReader: bufio.NewReaderSize(conn, connBufferSize),
	}
}

// newServerConn should be used to create server connections.
//
// It stashes a reference to the listener to be able to determine if
// the server is shutting down, and has the ability to control buffer
// size for reads.
func newServerConn(conn net.Conn, listener *Listener) *Conn {
	c := &Conn{
		conn:        conn,
		listener:    listener,
		closed:      sync2.NewAtomicBool(false),
		PrepareData: make(map[uint32]*PrepareData),
	}
	if listener.connReadBufferSize > 0 {
		c.bufferedReader = bufio.NewReaderSize(conn, listener.connReadBufferSize)
	}
	return c
}

// startWriterBuffering starts using buffered writes. This should
// be terminated by a call to flush.
func (c *Conn) startWriterBuffering() {
	c.bufferedWriter = writersPool.Get().(*bufio.Writer)
	c.bufferedWriter.Reset(c.conn)
}

// flush flushes the written data to the socket.
// This must be called to terminate startBuffering.
func (c *Conn) flush() error {
	if c.bufferedWriter == nil {
		return nil
	}

	defer func() {
		c.bufferedWriter.Reset(nil)
		writersPool.Put(c.bufferedWriter)
		c.bufferedWriter = nil
	}()

	return c.bufferedWriter.Flush()
}

// getWriter returns the current writer. It may be either
// the original connection or a wrapper.
func (c *Conn) getWriter() io.Writer {
	if c.bufferedWriter != nil {
		return c.bufferedWriter
	}
	return c.conn
}

// getReader returns reader for connection. It can be *bufio.Reader or net.Conn
// depending on which buffer size was passed to newServerConn.
func (c *Conn) getReader() io.Reader {
	if c.bufferedReader != nil {
		return c.bufferedReader
	}
	return c.conn
}

func (c *Conn) readHeaderFrom(r io.Reader) (int, error) {
	var header [4]byte
	// Note io.ReadFull will return two different types of errors:
	// 1. if the socket is already closed, and the go runtime knows it,
	//   then ReadFull will return an error (different than EOF),
	//   someting like 'read: connection reset by peer'.
	// 2. if the socket is not closed while we start the read,
	//   but gets closed after the read is started, we'll get io.EOF.
	if _, err := io.ReadFull(r, header[:]); err != nil {
		// The special casing of propagating io.EOF up
		// is used by the server side only, to suppress an error
		// message if a client just disconnects.
		if err == io.EOF {
			return 0, err
		}
		if strings.HasSuffix(err.Error(), "read: connection reset by peer") {
			return 0, io.EOF
		}
		return 0, vterrors.Wrapf(err, "io.ReadFull(header size) failed")
	}

	sequence := uint8(header[3])
	if sequence != c.sequence {
		return 0, vterrors.Errorf(vtrpc.Code_INTERNAL, "invalid sequence, expected %v got %v", c.sequence, sequence)
	}

	c.sequence++

	return int(uint32(header[0]) | uint32(header[1])<<8 | uint32(header[2])<<16), nil
}

// readEphemeralPacket attempts to read a packet into buffer from sync.Pool.  Do
// not use this method if the contents of the packet needs to be kept
// after the next readEphemeralPacket.
//
// Note if the connection is closed already, an error will be
// returned, and it may not be io.EOF. If the connection closes while
// we are stuck waiting for data, an error will also be returned, and
// it most likely will be io.EOF.
func (c *Conn) readEphemeralPacket() ([]byte, error) {
	if c.currentEphemeralPolicy != ephemeralUnused {
		panic(vterrors.Errorf(vtrpc.Code_INTERNAL, "readEphemeralPacket: unexpected currentEphemeralPolicy: %v", c.currentEphemeralPolicy))
	}

	r := c.getReader()

	length, err := c.readHeaderFrom(r)
	if err != nil {
		return nil, err
	}

	c.currentEphemeralPolicy = ephemeralRead
	if length == 0 {
		// This can be caused by the packet after a packet of
		// exactly size MaxPacketSize.
		return nil, nil
	}

	// Use the bufPool.
	if length < MaxPacketSize {
		c.currentEphemeralBuffer = bufPool.Get(length)
		if _, err := io.ReadFull(r, *c.currentEphemeralBuffer); err != nil {
			return nil, vterrors.Wrapf(err, "io.ReadFull(packet body of length %v) failed", length)
		}
		return *c.currentEphemeralBuffer, nil
	}

	// Much slower path, revert to allocating everything from scratch.
	// We're going to concatenate a lot of data anyway, can't really
	// optimize this code path easily.
	data := make([]byte, length)
	if _, err := io.ReadFull(r, data); err != nil {
		return nil, vterrors.Wrapf(err, "io.ReadFull(packet body of length %v) failed", length)
	}
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

// readEphemeralPacketDirect attempts to read a packet from the socket directly.
// It needs to be used for the first handshake packet the server receives,
// so we do't buffer the SSL negotiation packet. As a shortcut, only
// packets smaller than MaxPacketSize can be read here.
// This function usually shouldn't be used - use readEphemeralPacket.
func (c *Conn) readEphemeralPacketDirect() ([]byte, error) {
	if c.currentEphemeralPolicy != ephemeralUnused {
		panic(vterrors.Errorf(vtrpc.Code_INTERNAL, "readEphemeralPacketDirect: unexpected currentEphemeralPolicy: %v", c.currentEphemeralPolicy))
	}

	var r io.Reader = c.conn

	length, err := c.readHeaderFrom(r)
	if err != nil {
		return nil, err
	}

	c.currentEphemeralPolicy = ephemeralRead
	if length == 0 {
		// This can be caused by the packet after a packet of
		// exactly size MaxPacketSize.
		return nil, nil
	}

	if length < MaxPacketSize {
		c.currentEphemeralBuffer = bufPool.Get(length)
		if _, err := io.ReadFull(r, *c.currentEphemeralBuffer); err != nil {
			return nil, vterrors.Wrapf(err, "io.ReadFull(packet body of length %v) failed", length)
		}
		return *c.currentEphemeralBuffer, nil
	}

	return nil, vterrors.Errorf(vtrpc.Code_INTERNAL, "readEphemeralPacketDirect doesn't support more than one packet")
}

// recycleReadPacket recycles the read packet. It needs to be called
// after readEphemeralPacket was called.
func (c *Conn) recycleReadPacket() {
	if c.currentEphemeralPolicy != ephemeralRead {
		// Programming error.
		panic(vterrors.Errorf(vtrpc.Code_INTERNAL, "trying to call recycleReadPacket while currentEphemeralPolicy is %d", c.currentEphemeralPolicy))
	}
	if c.currentEphemeralBuffer != nil {
		// We are using the pool, put the buffer back in.
		bufPool.Put(c.currentEphemeralBuffer)
		c.currentEphemeralBuffer = nil
	}
	c.currentEphemeralPolicy = ephemeralUnused
}

// readOnePacket reads a single packet into a newly allocated buffer.
func (c *Conn) readOnePacket() ([]byte, error) {
	r := c.getReader()
	length, err := c.readHeaderFrom(r)
	if err != nil {
		return nil, err
	}
	if length == 0 {
		// This can be caused by the packet after a packet of
		// exactly size MaxPacketSize.
		return nil, nil
	}

	data := make([]byte, length)
	if _, err := io.ReadFull(r, data); err != nil {
		return nil, vterrors.Wrapf(err, "io.ReadFull(packet body of length %v) failed", length)
	}
	return data, nil
}

// readPacket reads a packet from the underlying connection.
// It re-assembles packets that span more than one message.
// This method returns a generic error, not a SQLError.
func (c *Conn) readPacket() ([]byte, error) {
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

// ReadPacket reads a packet from the underlying connection.
// it is the public API version, that returns a SQLError.
// The memory for the packet is always allocated, and it is owned by the caller
// after this function returns.
func (c *Conn) ReadPacket() ([]byte, error) {
	result, err := c.readPacket()
	if err != nil {
		return nil, NewSQLError(CRServerLost, SSUnknownSQLState, "%v", err)
	}
	return result, err
}

// writePacket writes a packet, possibly cutting it into multiple
// chunks.  Note this is not very efficient, as the client probably
// has to build the []byte and that makes a memory copy.
// Try to use startEphemeralPacket/writeEphemeralPacket instead.
//
// This method returns a generic error, not a SQLError.
func (c *Conn) writePacket(data []byte) error {
	index := 0
	length := len(data)

	w := c.getWriter()

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
		if n, err := w.Write(header[:]); err != nil {
			return vterrors.Wrapf(err, "Write(header) failed")
		} else if n != 4 {
			return vterrors.Errorf(vtrpc.Code_INTERNAL, "Write(header) returned a short write: %v < 4", n)
		}

		// Write the body.
		if n, err := w.Write(data[index : index+packetLength]); err != nil {
			return vterrors.Wrapf(err, "Write(packet) failed")
		} else if n != packetLength {
			return vterrors.Errorf(vtrpc.Code_INTERNAL, "Write(packet) returned a short write: %v < %v", n, packetLength)
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
				if n, err := w.Write(header[:]); err != nil {
					return vterrors.Wrapf(err, "Write(empty header) failed")
				} else if n != 4 {
					return vterrors.Errorf(vtrpc.Code_INTERNAL, "Write(empty header) returned a short write: %v < 4", n)
				}
				c.sequence++
			}
			return nil
		}
		index += packetLength
	}
}

func (c *Conn) startEphemeralPacket(length int) []byte {
	if c.currentEphemeralPolicy != ephemeralUnused {
		panic("startEphemeralPacket cannot be used while a packet is already started.")
	}

	c.currentEphemeralPolicy = ephemeralWrite
	// get buffer from pool or it'll be allocated if length is too big
	c.currentEphemeralBuffer = bufPool.Get(length)
	return *c.currentEphemeralBuffer
}

// writeEphemeralPacket writes the packet that was allocated by
// startEphemeralPacket.
func (c *Conn) writeEphemeralPacket() error {
	defer c.recycleWritePacket()

	switch c.currentEphemeralPolicy {
	case ephemeralWrite:
		if err := c.writePacket(*c.currentEphemeralBuffer); err != nil {
			return vterrors.Wrapf(err, "conn %v", c.ID())
		}
	case ephemeralUnused, ephemeralRead:
		// Programming error.
		panic(vterrors.Errorf(vtrpc.Code_INTERNAL, "conn %v: trying to call writeEphemeralPacket while currentEphemeralPolicy is %v", c.ID(), c.currentEphemeralPolicy))
	}

	return nil
}

// recycleWritePacket recycles the write packet. It needs to be called
// after writeEphemeralPacket was called.
func (c *Conn) recycleWritePacket() {
	if c.currentEphemeralPolicy != ephemeralWrite {
		// Programming error.
		panic(vterrors.Errorf(vtrpc.Code_INTERNAL, "trying to call recycleWritePacket while currentEphemeralPolicy is %d", c.currentEphemeralPolicy))
	}
	// Release our reference so the buffer can be gced
	bufPool.Put(c.currentEphemeralBuffer)
	c.currentEphemeralBuffer = nil
	c.currentEphemeralPolicy = ephemeralUnused
}

// writeComQuit writes a Quit message for the server, to indicate we
// want to close the connection.
// Client -> Server.
// Returns SQLError(CRServerGone) if it can't.
func (c *Conn) writeComQuit() error {
	// This is a new command, need to reset the sequence.
	c.sequence = 0

	data := c.startEphemeralPacket(1)
	data[0] = ComQuit
	if err := c.writeEphemeralPacket(); err != nil {
		return NewSQLError(CRServerGone, SSUnknownSQLState, err.Error())
	}
	return nil
}

// RemoteAddr returns the underlying socket RemoteAddr().
func (c *Conn) RemoteAddr() net.Addr {
	return c.conn.RemoteAddr()
}

// ID returns the MySQL connection ID for this connection.
func (c *Conn) ID() int64 {
	return int64(c.ConnectionID)
}

// Ident returns a useful identification string for error logging
func (c *Conn) String() string {
	return fmt.Sprintf("client %v (%s)", c.ConnectionID, c.RemoteAddr().String())
}

// Close closes the connection. It can be called from a different go
// routine to interrupt the current connection.
func (c *Conn) Close() {
	if c.closed.CompareAndSwap(false, true) {
		c.conn.Close()
	}
}

// IsClosed returns true if this connection was ever closed by the
// Close() method.  Note if the other side closes the connection, but
// Close() wasn't called, this will return false.
func (c *Conn) IsClosed() bool {
	return c.closed.Get()
}

//
// Packet writing methods, for generic packets.
//

// writeOKPacket writes an OK packet.
// Server -> Client.
// This method returns a generic error, not a SQLError.
func (c *Conn) writeOKPacket(affectedRows, lastInsertID uint64, flags uint16, warnings uint16) error {
	length := 1 + // OKPacket
		lenEncIntSize(affectedRows) +
		lenEncIntSize(lastInsertID) +
		2 + // flags
		2 // warnings
	data := c.startEphemeralPacket(length)
	pos := 0
	pos = writeByte(data, pos, OKPacket)
	pos = writeLenEncInt(data, pos, affectedRows)
	pos = writeLenEncInt(data, pos, lastInsertID)
	pos = writeUint16(data, pos, flags)
	_ = writeUint16(data, pos, warnings)

	return c.writeEphemeralPacket()
}

// writeOKPacketWithEOFHeader writes an OK packet with an EOF header.
// This is used at the end of a result set if
// CapabilityClientDeprecateEOF is set.
// Server -> Client.
// This method returns a generic error, not a SQLError.
func (c *Conn) writeOKPacketWithEOFHeader(affectedRows, lastInsertID uint64, flags uint16, warnings uint16) error {
	length := 1 + // EOFPacket
		lenEncIntSize(affectedRows) +
		lenEncIntSize(lastInsertID) +
		2 + // flags
		2 // warnings
	data := c.startEphemeralPacket(length)
	pos := 0
	pos = writeByte(data, pos, EOFPacket)
	pos = writeLenEncInt(data, pos, affectedRows)
	pos = writeLenEncInt(data, pos, lastInsertID)
	pos = writeUint16(data, pos, flags)
	_ = writeUint16(data, pos, warnings)

	return c.writeEphemeralPacket()
}

// writeErrorPacket writes an error packet.
// Server -> Client.
// This method returns a generic error, not a SQLError.
func (c *Conn) writeErrorPacket(errorCode uint16, sqlState string, format string, args ...interface{}) error {
	errorMessage := fmt.Sprintf(format, args...)
	length := 1 + 2 + 1 + 5 + len(errorMessage)
	data := c.startEphemeralPacket(length)
	pos := 0
	pos = writeByte(data, pos, ErrPacket)
	pos = writeUint16(data, pos, errorCode)
	pos = writeByte(data, pos, '#')
	if sqlState == "" {
		sqlState = SSUnknownSQLState
	}
	if len(sqlState) != 5 {
		panic("sqlState has to be 5 characters long")
	}
	pos = writeEOFString(data, pos, sqlState)
	_ = writeEOFString(data, pos, errorMessage)

	return c.writeEphemeralPacket()
}

// writeErrorPacketFromError writes an error packet, from a regular error.
// See writeErrorPacket for other info.
func (c *Conn) writeErrorPacketFromError(err error) error {
	if se, ok := err.(*SQLError); ok {
		return c.writeErrorPacket(uint16(se.Num), se.State, "%v", se.Message)
	}

	return c.writeErrorPacket(ERUnknownError, SSUnknownSQLState, "unknown error: %v", err)
}

// writeEOFPacket writes an EOF packet, through the buffer, and
// doesn't flush (as it is used as part of a query result).
func (c *Conn) writeEOFPacket(flags uint16, warnings uint16) error {
	length := 5
	data := c.startEphemeralPacket(length)
	pos := 0
	pos = writeByte(data, pos, EOFPacket)
	pos = writeUint16(data, pos, warnings)
	_ = writeUint16(data, pos, flags)

	return c.writeEphemeralPacket()
}

// handleNextCommand is called in the server loop to process
// incoming packets.
func (c *Conn) handleNextCommand(handler Handler) error {
	c.sequence = 0
	data, err := c.readEphemeralPacket()
	if err != nil {
		// Don't log EOF errors. They cause too much spam.
		// Note the EOF detection is not 100%
		// guaranteed, in the case where the client
		// connection is already closed before we call
		// 'readEphemeralPacket'.  This is a corner
		// case though, and very unlikely to happen,
		// and the only downside is we log a bit more then.
		if err != io.EOF {
			log.Errorf("Error reading packet from %s: %v", c, err)
		}
		return err
	}

	switch data[0] {
	case ComQuit:
		c.recycleReadPacket()
		return errors.New("ComQuit")
	case ComInitDB:
		db := c.parseComInitDB(data)
		c.recycleReadPacket()
		c.SchemaName = db
		if err := c.writeOKPacket(0, 0, c.StatusFlags, 0); err != nil {
			log.Errorf("Error writing ComInitDB result to %s: %v", c, err)
			return err
		}
	case ComQuery:
		// flush is called at the end of this block.
		// We cannot encapsulate it with a defer inside a func because
		// we have to return from this func if it fails.
		c.startWriterBuffering()

		queryStart := time.Now()
		query := c.parseComQuery(data)
		c.recycleReadPacket()

		var queries []string
		if c.Capabilities&CapabilityClientMultiStatements != 0 {
			queries, err = sqlparser.SplitStatementToPieces(query)
			if err != nil {
				log.Errorf("Conn %v: Error splitting query: %v", c, err)
				if werr := c.writeErrorPacketFromError(err); werr != nil {
					// If we can't even write the error, we're done.
					log.Errorf("Conn %v: Error writing query error: %v", c, werr)
					return werr
				}
			}
		} else {
			queries = []string{query}
		}
		for index, sql := range queries {
			more := false
			if index != len(queries)-1 {
				more = true
			}
			if err := c.execQuery(sql, handler, more); err != nil {
				return err
			}
		}

		timings.Record(queryTimingKey, queryStart)

		if err := c.flush(); err != nil {
			log.Errorf("Conn %v: Flush() failed: %v", c.ID(), err)
			return err
		}

	case ComPing:
		c.recycleReadPacket()
		// Return error if listener was shut down and OK otherwise
		if c.listener.isShutdown() {
			if err := c.writeErrorPacket(ERServerShutdown, SSServerShutdown, "Server shutdown in progress"); err != nil {
				log.Errorf("Error writing ComPing error to %s: %v", c, err)
				return err
			}
		} else {
			if err := c.writeOKPacket(0, 0, c.StatusFlags, 0); err != nil {
				log.Errorf("Error writing ComPing result to %s: %v", c, err)
				return err
			}
		}
	case ComSetOption:
		operation, ok := c.parseComSetOption(data)
		c.recycleReadPacket()
		if ok {
			switch operation {
			case 0:
				c.Capabilities |= CapabilityClientMultiStatements
			case 1:
				c.Capabilities &^= CapabilityClientMultiStatements
			default:
				log.Errorf("Got unhandled packet (ComSetOption default) from client %v, returning error: %v", c.ConnectionID, data)
				if err := c.writeErrorPacket(ERUnknownComError, SSUnknownComError, "error handling packet: %v", data); err != nil {
					log.Errorf("Error writing error packet to client: %v", err)
					return err
				}
			}
			if err := c.writeEndResult(false, 0, 0, 0); err != nil {
				log.Errorf("Error writeEndResult error %v ", err)
				return err
			}
		} else {
			log.Errorf("Got unhandled packet (ComSetOption else) from client %v, returning error: %v", c.ConnectionID, data)
			if err := c.writeErrorPacket(ERUnknownComError, SSUnknownComError, "error handling packet: %v", data); err != nil {
				log.Errorf("Error writing error packet to client: %v", err)
				return err
			}
		}
	case ComPrepare:
		query := c.parseComPrepare(data)
		c.recycleReadPacket()

		var queries []string
		if c.Capabilities&CapabilityClientMultiStatements != 0 {
			queries, err = sqlparser.SplitStatementToPieces(query)
			if err != nil {
				log.Errorf("Conn %v: Error splitting query: %v", c, err)
				if werr := c.writeErrorPacketFromError(err); werr != nil {
					// If we can't even write the error, we're done.
					log.Errorf("Conn %v: Error writing query error: %v", c, werr)
					return werr
				}
			}
		} else {
			queries = []string{query}
		}

		if len(queries) != 1 {
			return fmt.Errorf("can not prepare multiple statements")
		}

		// Popoulate PrepareData
		c.StatementID++
		prepare := &PrepareData{
			StatementID: c.StatementID,
			PrepareStmt: queries[0],
		}

		statement, err := sqlparser.ParseStrictDDL(query)
		if err != nil {
			return err
		}

		paramsCount := uint16(0)
		_ = sqlparser.Walk(func(node sqlparser.SQLNode) (bool, error) {
			switch node := node.(type) {
			case *sqlparser.SQLVal:
				if strings.HasPrefix(string(node.Val), ":v") {
					paramsCount++
				}
			}
			return true, nil
		}, statement)

		if paramsCount > 0 {
			prepare.ParamsCount = paramsCount
			prepare.ParamsType = make([]int32, paramsCount)
			prepare.BindVars = make(map[string]*querypb.BindVariable, paramsCount)
		}

		c.PrepareData[c.StatementID] = prepare

		fld, err := handler.ComPrepare(c, queries[0])

		if err != nil {
			if werr := c.writeErrorPacketFromError(err); werr != nil {
				// If we can't even write the error, we're done.
				log.Error("Error writing query error to client %v: %v", c.ConnectionID, werr)
				return werr
			}
			return nil
		}

		if err := c.writePrepare(fld, c.PrepareData[c.StatementID]); err != nil {
			return err
		}

	case ComStmtExecute:
		queryStart := time.Now()
		stmtID, _, err := c.parseComStmtExecute(c.PrepareData, data)
		c.recycleReadPacket()

		if stmtID != uint32(0) {
			defer func() {
				prepare := c.PrepareData[stmtID]
				if prepare.BindVars != nil {
					for k := range prepare.BindVars {
						prepare.BindVars[k] = nil
					}
				}
			}()
		}

		if err != nil {
			if werr := c.writeErrorPacketFromError(err); werr != nil {
				// If we can't even write the error, we're done.
				log.Error("Error writing query error to client %v: %v", c.ConnectionID, werr)
				return werr
			}
			return nil
		}

		fieldSent := false
		// sendFinished is set if the response should just be an OK packet.
		sendFinished := false
		prepare := c.PrepareData[stmtID]
		err = handler.ComStmtExecute(c, prepare, func(qr *sqltypes.Result) error {
			if sendFinished {
				// Failsafe: Unreachable if server is well-behaved.
				return io.EOF
			}

			if !fieldSent {
				fieldSent = true

				if len(qr.Fields) == 0 {
					sendFinished = true
					// We should not send any more packets after this.
					return c.writeOKPacket(qr.RowsAffected, qr.InsertID, c.StatusFlags, 0)
				}
				if err := c.writeFields(qr); err != nil {
					return err
				}
			}

			return c.writeBinaryRows(qr)
		})

		// If no field was sent, we expect an error.
		if !fieldSent {
			// This is just a failsafe. Should never happen.
			if err == nil || err == io.EOF {
				err = NewSQLErrorFromError(errors.New("unexpected: query ended without no results and no error"))
			}
			if werr := c.writeErrorPacketFromError(err); werr != nil {
				// If we can't even write the error, we're done.
				log.Errorf("Error writing query error to %s: %v", c, werr)
				return werr
			}
		} else {
			if err != nil {
				// We can't send an error in the middle of a stream.
				// All we can do is abort the send, which will cause a 2013.
				log.Errorf("Error in the middle of a stream to %s: %v", c, err)
				return err
			}

			// Send the end packet only sendFinished is false (results were streamed).
			// In this case the affectedRows and lastInsertID are always 0 since it
			// was a read operation.
			if !sendFinished {
				if err := c.writeEndResult(false, 0, 0, handler.WarningCount(c)); err != nil {
					log.Errorf("Error writing result to %s: %v", c, err)
					return err
				}
			}
		}

		timings.Record(queryTimingKey, queryStart)
	case ComStmtSendLongData:
		stmtID, paramID, chunkData, ok := c.parseComStmtSendLongData(data)
		c.recycleReadPacket()
		if !ok {
			err := fmt.Errorf("error parsing statement send long data from client %v, returning error: %v", c.ConnectionID, data)
			log.Error(err.Error())
			return err
		}

		prepare, ok := c.PrepareData[stmtID]
		if !ok {
			err := fmt.Errorf("got wrong statement id from client %v, statement ID(%v) is not found from record", c.ConnectionID, stmtID)
			log.Error(err.Error())
			return err
		}

		if prepare.BindVars == nil ||
			prepare.ParamsCount == uint16(0) ||
			paramID >= prepare.ParamsCount {
			err := fmt.Errorf("invalid parameter Number from client %v, statement: %v", c.ConnectionID, prepare.PrepareStmt)
			log.Error(err.Error())
			return err
		}

		chunk := make([]byte, len(chunkData))
		copy(chunk, chunkData)

		key := fmt.Sprintf("v%d", paramID+1)
		if val, ok := prepare.BindVars[key]; ok {
			val.Value = append(val.Value, chunk...)
		} else {
			prepare.BindVars[key] = sqltypes.BytesBindVariable(chunk)
		}
	case ComStmtClose:
		stmtID, ok := c.parseComStmtClose(data)
		c.recycleReadPacket()
		if ok {
			delete(c.PrepareData, stmtID)
		}
	case ComStmtReset:
		stmtID, ok := c.parseComStmtReset(data)
		c.recycleReadPacket()
		if !ok {
			log.Error("Got unhandled packet from client %v, returning error: %v", c.ConnectionID, data)
			if err := c.writeErrorPacket(ERUnknownComError, SSUnknownComError, "error handling packet: %v", data); err != nil {
				log.Error("Error writing error packet to client: %v", err)
				return err
			}
		}

		prepare, ok := c.PrepareData[stmtID]
		if !ok {
			log.Error("Commands were executed in an improper order from client %v, packet: %v", c.ConnectionID, data)
			if err := c.writeErrorPacket(CRCommandsOutOfSync, SSUnknownComError, "commands were executed in an improper order: %v", data); err != nil {
				log.Error("Error writing error packet to client: %v", err)
				return err
			}
		}

		if prepare.BindVars != nil {
			for k := range prepare.BindVars {
				prepare.BindVars[k] = nil
			}
		}

		if err := c.writeOKPacket(0, 0, c.StatusFlags, 0); err != nil {
			log.Error("Error writing ComStmtReset OK packet to client %v: %v", c.ConnectionID, err)
			return err
		}
	default:
		log.Errorf("Got unhandled packet (default) from %s, returning error: %v", c, data)
		c.recycleReadPacket()
		if err := c.writeErrorPacket(ERUnknownComError, SSUnknownComError, "command handling not implemented yet: %v", data[0]); err != nil {
			log.Errorf("Error writing error packet to %s: %s", c, err)
			return err
		}
	}

	return nil
}

func (c *Conn) execQuery(query string, handler Handler, more bool) error {
	fieldSent := false
	// sendFinished is set if the response should just be an OK packet.
	sendFinished := false

	err := handler.ComQuery(c, query, func(qr *sqltypes.Result) error {
		flag := c.StatusFlags
		if more {
			flag |= ServerMoreResultsExists
		}
		if sendFinished {
			// Failsafe: Unreachable if server is well-behaved.
			return io.EOF
		}

		if !fieldSent {
			fieldSent = true

			if len(qr.Fields) == 0 {
				sendFinished = true

				// A successful callback with no fields means that this was a
				// DML or other write-only operation.
				//
				// We should not send any more packets after this, but make sure
				// to extract the affected rows and last insert id from the result
				// struct here since clients expect it.
				return c.writeOKPacket(qr.RowsAffected, qr.InsertID, flag, handler.WarningCount(c))
			}
			if err := c.writeFields(qr); err != nil {
				return err
			}
		}

		return c.writeRows(qr)
	})

	// If no field was sent, we expect an error.
	if !fieldSent {
		// This is just a failsafe. Should never happen.
		if err == nil || err == io.EOF {
			err = NewSQLErrorFromError(errors.New("unexpected: query ended without no results and no error"))
		}
		if werr := c.writeErrorPacketFromError(err); werr != nil {
			// If we can't even write the error, we're done.
			log.Errorf("Error writing query error to %s: %v", c, werr)
			return werr
		}
	} else {
		if err != nil {
			// We can't send an error in the middle of a stream.
			// All we can do is abort the send, which will cause a 2013.
			log.Errorf("Error in the middle of a stream to %s: %v", c, err)
			return err
		}

		// Send the end packet only sendFinished is false (results were streamed).
		// In this case the affectedRows and lastInsertID are always 0 since it
		// was a read operation.
		if !sendFinished {
			if err := c.writeEndResult(more, 0, 0, handler.WarningCount(c)); err != nil {
				log.Errorf("Error writing result to %s: %v", c, err)
				return err
			}
		}
	}

	return nil
}

//
// Packet parsing methods, for generic packets.
//

// isEOFPacket determines whether or not a data packet is a "true" EOF. DO NOT blindly compare the
// first byte of a packet to EOFPacket as you might do for other packet types, as 0xfe is overloaded
// as a first byte.
//
// Per https://dev.mysql.com/doc/internals/en/packet-EOF_Packet.html, a packet starting with 0xfe
// but having length >= 9 (on top of 4 byte header) is not a true EOF but a LengthEncodedInteger
// (typically preceding a LengthEncodedString). Thus, all EOF checks must validate the payload size
// before exiting.
//
// More specifically, an EOF packet can have 3 different lengths (1, 5, 7) depending on the client
// flags that are set. 7 comes from server versions of 5.7.5 or greater where ClientDeprecateEOF is
// set (i.e. uses an OK packet starting with 0xfe instead of 0x00 to signal EOF). Regardless, 8 is
// an upper bound otherwise it would be ambiguous w.r.t. LengthEncodedIntegers.
//
// More docs here:
// https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_basic_response_packets.html
func isEOFPacket(data []byte) bool {
	return data[0] == EOFPacket && len(data) < 9
}

// parseEOFPacket returns the warning count and a boolean to indicate if there
// are more results to receive.
//
// Note: This is only valid on actual EOF packets and not on OK packets with the EOF
// type code set, i.e. should not be used if ClientDeprecateEOF is set.
func parseEOFPacket(data []byte) (warnings uint16, more bool, err error) {
	// The warning count is in position 2 & 3
	warnings, _, _ = readUint16(data, 1)

	// The status flag is in position 4 & 5
	statusFlags, _, ok := readUint16(data, 3)
	if !ok {
		return 0, false, vterrors.Errorf(vtrpc.Code_INTERNAL, "invalid EOF packet statusFlags: %v", data)
	}
	return warnings, (statusFlags & ServerMoreResultsExists) != 0, nil
}

func parseOKPacket(data []byte) (uint64, uint64, uint16, uint16, error) {
	// We already read the type.
	pos := 1

	// Affected rows.
	affectedRows, pos, ok := readLenEncInt(data, pos)
	if !ok {
		return 0, 0, 0, 0, vterrors.Errorf(vtrpc.Code_INTERNAL, "invalid OK packet affectedRows: %v", data)
	}

	// Last Insert ID.
	lastInsertID, pos, ok := readLenEncInt(data, pos)
	if !ok {
		return 0, 0, 0, 0, vterrors.Errorf(vtrpc.Code_INTERNAL, "invalid OK packet lastInsertID: %v", data)
	}

	// Status flags.
	statusFlags, pos, ok := readUint16(data, pos)
	if !ok {
		return 0, 0, 0, 0, vterrors.Errorf(vtrpc.Code_INTERNAL, "invalid OK packet statusFlags: %v", data)
	}

	// Warnings.
	warnings, _, ok := readUint16(data, pos)
	if !ok {
		return 0, 0, 0, 0, vterrors.Errorf(vtrpc.Code_INTERNAL, "invalid OK packet warnings: %v", data)
	}

	return affectedRows, lastInsertID, statusFlags, warnings, nil
}

// isErrorPacket determines whether or not the packet is an error packet. Mostly here for
// consistency with isEOFPacket
func isErrorPacket(data []byte) bool {
	return data[0] == ErrPacket
}

// ParseErrorPacket parses the error packet and returns a SQLError.
func ParseErrorPacket(data []byte) error {
	// We already read the type.
	pos := 1

	// Error code is 2 bytes.
	code, pos, ok := readUint16(data, pos)
	if !ok {
		return NewSQLError(CRUnknownError, SSUnknownSQLState, "invalid error packet code: %v", data)
	}

	// '#' marker of the SQL state is 1 byte. Ignored.
	pos++

	// SQL state is 5 bytes
	sqlState, pos, ok := readBytes(data, pos, 5)
	if !ok {
		return NewSQLError(CRUnknownError, SSUnknownSQLState, "invalid error packet sqlState: %v", data)
	}

	// Human readable error message is the rest.
	msg := string(data[pos:])

	return NewSQLError(int(code), string(sqlState), "%v", msg)
}

// GetTLSClientCerts gets TLS certificates.
func (c *Conn) GetTLSClientCerts() []*x509.Certificate {
	if tlsConn, ok := c.conn.(*tls.Conn); ok {
		return tlsConn.ConnectionState().PeerCertificates
	}
	return nil
}
