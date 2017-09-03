/*
Copyright 2017 Google Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreedto in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package mysql

import (
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	"time"

	log "github.com/golang/glog"
	"github.com/youtube/vitess/go/sqltypes"
	"github.com/youtube/vitess/go/stats"
	"github.com/youtube/vitess/go/tb"
)

const (
	// DefaultServerVersion is the default server version we're sending to the client.
	// Can be changed.
	DefaultServerVersion = "5.5.10-Vitess"

	// timing metric keys
	connectTimingKey = "Connect"
	queryTimingKey   = "Query"
)

var (
	// Metrics
	timings    = stats.NewTimings("MysqlServerTimings")
	connCount  = stats.NewInt("MysqlServerConnCount")
	connAccept = stats.NewInt("MysqlServerConnAccepted")
	connSlow   = stats.NewInt("MysqlServerConnSlow")
)

// A Handler is an interface used by Listener to send queries.
// The implementation of this interface may store data in the ClientData
// field of the Connection for its own purposes.
//
// For a given Connection, all these methods are serialized. It means
// only one of these methods will be called concurrently for a given
// Connection. So access to the Connection ClientData does not need to
// be protected by a mutex.
//
// However, each connection is using one go routine, so multiple
// Connection objects can call these concurrently, for different Connections.
type Handler interface {
	// NewConnection is called when a connection is created.
	// It is not established yet. The handler can decide to
	// set StatusFlags that will be returned by the handshake methods.
	// In particular, ServerStatusAutocommit might be set.
	NewConnection(c *Conn)

	// ConnectionClosed is called when a connection is closed.
	ConnectionClosed(c *Conn)

	// ComQuery is called when a connection receives a query.
	// Note the contents of the query slice may change after
	// the first call to callback. So the Handler should not
	// hang on to the byte slice.
	ComQuery(c *Conn, query []byte, callback func(*sqltypes.Result) error) error
}

// Listener is the MySQL server protocol listener.
type Listener struct {
	// Construction parameters, set by NewListener.

	// authServer is the AuthServer object to use for authentication.
	authServer AuthServer

	// handler is the data handler.
	handler Handler

	// This is the main listener socket.
	listener net.Listener

	// The following parameters are read by multiple connection go
	// routines.  They are not protected by a mutex, so they
	// should be set after NewListener, and not changed while
	// Accept is running.

	// ServerVersion is the version we will advertise.
	ServerVersion string

	// TLSConfig is the server TLS config. If set, we will advertise
	// that we support SSL.
	TLSConfig *tls.Config

	// AllowClearTextWithoutTLS needs to be set for the
	// mysql_clear_password authentication method to be accepted
	// by the server when TLS is not in use.
	AllowClearTextWithoutTLS bool

	// SlowConnectWarnThreshold if non-zero specifies an amount of time
	// beyond which a warning is logged to identify the slow connection
	SlowConnectWarnThreshold time.Duration

	// The following parameters are changed by the Accept routine.

	// Incrementing ID for connection id.
	connectionID uint32
}

// NewListener creates a new Listener.
func NewListener(protocol, address string, authServer AuthServer, handler Handler) (*Listener, error) {
	listener, err := net.Listen(protocol, address)
	if err != nil {
		return nil, err
	}

	return &Listener{
		authServer: authServer,
		handler:    handler,
		listener:   listener,

		ServerVersion: DefaultServerVersion,
		connectionID:  1,
	}, nil
}

// Addr returns the listener address.
func (l *Listener) Addr() net.Addr {
	return l.listener.Addr()
}

// Accept runs an accept loop until the listener is closed.
func (l *Listener) Accept() {
	for {
		conn, err := l.listener.Accept()
		if err != nil {
			// Close() was probably called.
			return
		}

		acceptTime := time.Now()

		connectionID := l.connectionID
		l.connectionID++

		connCount.Add(1)
		connAccept.Add(1)

		go l.handle(conn, connectionID, acceptTime)
	}
}

// handle is called in a go routine for each client connection.
// FIXME(alainjobart) handle per-connection logs in a way that makes sense.
// FIXME(alainjobart) add an idle timeout for the connection.
func (l *Listener) handle(conn net.Conn, connectionID uint32, acceptTime time.Time) {
	c := newConn(conn)
	c.ConnectionID = connectionID

	// Catch panics, and close the connection in any case.
	defer func() {
		if x := recover(); x != nil {
			log.Errorf("mysql_server caught panic:\n%v\n%s", x, tb.Stack(4))
		}
		conn.Close()
	}()

	// Tell the handler about the connection coming and going.
	l.handler.NewConnection(c)
	defer l.handler.ConnectionClosed(c)

	// Adjust the count of open connections
	defer connCount.Add(-1)

	// First build and send the server handshake packet.
	salt, err := c.writeHandshakeV10(l.ServerVersion, l.authServer, l.TLSConfig != nil)
	if err != nil {
		log.Errorf("Cannot send HandshakeV10 packet to %s: %v", c, err)
		return
	}

	// Wait for the client response. This has to be a direct read,
	// so we don't buffer the TLS negotiation packets.
	response, err := c.readPacketDirect()
	if err != nil {
		log.Errorf("Cannot read client handshake response from %s: %v", c, err)
		return
	}
	user, authMethod, authResponse, err := l.parseClientHandshakePacket(c, true, response)
	if err != nil {
		log.Errorf("Cannot parse client handshake response from %s: %v", c, err)
		return
	}

	if c.Capabilities&CapabilityClientSSL > 0 {
		// SSL was enabled. We need to re-read the auth packet.
		response, err = c.readEphemeralPacket()
		if err != nil {
			log.Errorf("Cannot read post-SSL client handshake response from %s: %v", c, err)
			return
		}

		// Returns copies of the data, so we can recycle the buffer.
		user, authMethod, authResponse, err = l.parseClientHandshakePacket(c, false, response)
		if err != nil {
			log.Errorf("Cannot parse post-SSL client handshake response from %s: %v", c, err)
			return
		}
		c.recycleReadPacket()
	}

	// See what auth method the AuthServer wants to use for that user.
	authServerMethod, err := l.authServer.AuthMethod(user)
	if err != nil {
		c.writeErrorPacketFromError(err)
		return
	}

	// Compare with what the client sent back.
	switch {
	case authServerMethod == MysqlNativePassword && authMethod == MysqlNativePassword:
		// Both server and client want to use MysqlNativePassword:
		// the negotiation can be completed right away, using the
		// ValidateHash() method.
		userData, err := l.authServer.ValidateHash(salt, user, authResponse, conn.RemoteAddr())
		if err != nil {
			log.Warningf("Error authenticating user using MySQL native password: %v", err)
			c.writeErrorPacketFromError(err)
			return
		}
		c.User = user
		c.UserData = userData

	case authServerMethod == MysqlNativePassword:
		// The server really wants to use MysqlNativePassword,
		// but the client returned a result for something else:
		// not sure this can happen, so not supporting this now.
		c.writeErrorPacket(CRServerHandshakeErr, SSUnknownSQLState, "Client asked for auth %v, but server wants auth mysql_native_password", authMethod)
		return

	default:
		// The server wants to use something else, re-negotiate.

		// The negotiation happens in clear text. Let's check we can.
		if !l.AllowClearTextWithoutTLS && c.Capabilities&CapabilityClientSSL == 0 {
			c.writeErrorPacket(CRServerHandshakeErr, SSUnknownSQLState, "Cannot use clear text authentication over non-SSL connections.")
			return
		}

		// Switch our auth method to what the server wants.
		// Dialog plugin expects an AskPassword prompt.
		var data []byte
		if authServerMethod == MysqlDialog {
			data = authServerDialogSwitchData()
		}
		if err := c.writeAuthSwitchRequest(authServerMethod, data); err != nil {
			log.Errorf("Error writing auth switch packet for %s: %v", c, err)
			return
		}

		// Then hand over the rest of the negotiation to the
		// auth server.
		userData, err := l.authServer.Negotiate(c, user, conn.RemoteAddr())
		if err != nil {
			c.writeErrorPacketFromError(err)
			return
		}
		c.User = user
		c.UserData = userData
	}

	// Negotiation worked, send OK packet.
	if err := c.writeOKPacket(0, 0, c.StatusFlags, 0); err != nil {
		log.Errorf("Cannot write OK packet to %s: %v", c, err)
		return
	}

	// Record how long we took to establish the connection
	timings.Record(connectTimingKey, acceptTime)

	// Log a warning if it took too long to connect
	connectTime := time.Since(acceptTime)
	if l.SlowConnectWarnThreshold != 0 && connectTime > l.SlowConnectWarnThreshold {
		connSlow.Add(1)
		log.Warningf("Slow connection from %s: %v", c, connectTime)
	}

	for {
		c.sequence = 0
		data, err := c.readEphemeralPacket()
		if err != nil {
			// Don't log EOF errors. They cause too much spam.
			if err != io.EOF {
				log.Errorf("Error reading packet from %s: %v", c, err)
			}
			return
		}

		switch data[0] {
		case ComQuit:
			c.recycleReadPacket()
			return
		case ComInitDB:
			db := c.parseComInitDB(data)
			c.recycleReadPacket()
			c.SchemaName = db
			if err := c.writeOKPacket(0, 0, c.StatusFlags, 0); err != nil {
				log.Errorf("Error writing ComInitDB result to %s: %v", c, err)
				return
			}
		case ComQuery:
			queryStart := time.Now()
			query := c.parseComQuery(data)
			fieldSent := false
			// sendFinished is set if a response has completed and
			// no further packets must be sent.
			sendFinished := false
			err := l.handler.ComQuery(c, query, func(qr *sqltypes.Result) error {
				if sendFinished {
					// Failsafe: Unreachable if server is well-behaved.
					return io.EOF
				}

				if !fieldSent {
					c.recycleReadPacket()
					fieldSent = true

					if len(qr.Fields) == 0 {
						// We should not send any more packets after this.
						sendFinished = true
					}
					if err := c.writeFields(qr); err != nil {
						return err
					}
				}

				return c.writeRows(qr)
			})

			// If no field was sent, we expect an error.
			if !fieldSent {
				c.recycleReadPacket()
				// This is just a failsafe. Should never happen.
				if err == nil || err == io.EOF {
					err = NewSQLErrorFromError(errors.New("unexpected: query ended without no results and no error"))
				}
				if werr := c.writeErrorPacketFromError(err); werr != nil {
					// If we can't even write the error, we're done.
					log.Errorf("Error writing query error to %s: %v", c, werr)
					return
				}
				continue
			}

			if err != nil {
				// We can't send an error in the middle of a stream.
				// All we can do is abort the send, which will cause a 2013.
				log.Errorf("Error in the middle of a stream to %s: %v", c, err)
				return
			}

			// Send the end packet only is sendFinished is false (not a DML).
			if !sendFinished {
				if err := c.writeEndResult(); err != nil {
					log.Errorf("Error writing result to %s: %v", c, err)
					return
				}
			}

			timings.Record(queryTimingKey, queryStart)

		case ComPing:
			// No payload to that one, just return OKPacket.
			c.recycleReadPacket()
			if err := c.writeOKPacket(0, 0, c.StatusFlags, 0); err != nil {
				log.Errorf("Error writing ComPing result to %s: %v", c, err)
				return
			}
		default:
			log.Errorf("Got unhandled packet from %s, returning error: %v", c, data)
			c.recycleReadPacket()
			if err := c.writeErrorPacket(ERUnknownComError, SSUnknownComError, "command handling not implemented yet: %v", data[0]); err != nil {
				log.Errorf("Error writing error packet to %s: %s", c, err)
				return
			}

		}
	}
}

// Close stops the listener, and closes all connections.
func (l *Listener) Close() {
	l.listener.Close()
}

// writeHandshakeV10 writes the Initial Handshake Packet, server side.
// It returns the salt data.
func (c *Conn) writeHandshakeV10(serverVersion string, authServer AuthServer, enableTLS bool) ([]byte, error) {
	capabilities := CapabilityClientLongPassword |
		CapabilityClientLongFlag |
		CapabilityClientConnectWithDB |
		CapabilityClientProtocol41 |
		CapabilityClientTransactions |
		CapabilityClientSecureConnection |
		CapabilityClientPluginAuth |
		CapabilityClientPluginAuthLenencClientData |
		CapabilityClientDeprecateEOF
	if enableTLS {
		capabilities |= CapabilityClientSSL
	}

	length :=
		1 + // protocol version
			lenNullString(serverVersion) +
			4 + // connection ID
			8 + // first part of salt data
			1 + // filler byte
			2 + // capability flags (lower 2 bytes)
			1 + // character set
			2 + // status flag
			2 + // capability flags (upper 2 bytes)
			1 + // length of auth plugin data
			10 + // reserved (0)
			13 + // auth-plugin-data
			lenNullString(MysqlNativePassword) // auth-plugin-name

	data := c.startEphemeralPacket(length)
	pos := 0

	// Protocol version.
	pos = writeByte(data, pos, protocolVersion)

	// Copy server version.
	pos = writeNullString(data, pos, serverVersion)

	// Add connectionID in.
	pos = writeUint32(data, pos, c.ConnectionID)

	// Generate the salt, put 8 bytes in.
	salt, err := authServer.Salt()
	if err != nil {
		return nil, err
	}

	pos += copy(data[pos:], salt[:8])

	// One filler byte, always 0.
	pos = writeByte(data, pos, 0)

	// Lower part of the capability flags.
	pos = writeUint16(data, pos, uint16(capabilities))

	// Character set.
	pos = writeByte(data, pos, CharacterSetUtf8)

	// Status flag.
	pos = writeUint16(data, pos, c.StatusFlags)

	// Upper part of the capability flags.
	pos = writeUint16(data, pos, uint16(capabilities>>16))

	// Length of auth plugin data.
	// Always 21 (8 + 13).
	pos = writeByte(data, pos, 21)

	// Reserved
	pos += 10

	// Second part of auth plugin data.
	pos += copy(data[pos:], salt[8:])
	data[pos] = 0
	pos++

	// Copy authPluginName. We always start with mysql_native_password.
	pos = writeNullString(data, pos, MysqlNativePassword)

	// Sanity check.
	if pos != len(data) {
		return nil, fmt.Errorf("error building Handshake packet: got %v bytes expected %v", pos, len(data))
	}

	if err := c.writeEphemeralPacket(true); err != nil {
		return nil, err
	}

	return salt, nil
}

// parseClientHandshakePacket parses the handshake sent by the client.
// Returns the username, auth method, auth data, error.
// The original data is not pointed at, and can be freed.
func (l *Listener) parseClientHandshakePacket(c *Conn, firstTime bool, data []byte) (string, string, []byte, error) {
	pos := 0

	// Client flags, 4 bytes.
	clientFlags, pos, ok := readUint32(data, pos)
	if !ok {
		return "", "", nil, fmt.Errorf("parseClientHandshakePacket: can't read client flags")
	}
	if clientFlags&CapabilityClientProtocol41 == 0 {
		return "", "", nil, fmt.Errorf("parseClientHandshakePacket: only support protocol 4.1")
	}

	// Remember a subset of the capabilities, so we can use them
	// later in the protocol. If we re-received the handshake packet
	// after SSL negotiation, do not overwrite capabilities.
	if firstTime {
		c.Capabilities = clientFlags & (CapabilityClientDeprecateEOF | CapabilityClientFoundRows)
	}

	// Max packet size. Don't do anything with this now.
	// See doc.go for more information.
	_, pos, ok = readUint32(data, pos)
	if !ok {
		return "", "", nil, fmt.Errorf("parseClientHandshakePacket: can't read maxPacketSize")
	}

	// Character set. Need to handle it.
	characterSet, pos, ok := readByte(data, pos)
	if !ok {
		return "", "", nil, fmt.Errorf("parseClientHandshakePacket: can't read characterSet")
	}
	c.CharacterSet = characterSet

	// 23x reserved zero bytes.
	pos += 23

	// Check for SSL.
	if firstTime && l.TLSConfig != nil && clientFlags&CapabilityClientSSL > 0 {
		// Need to switch to TLS, and then re-read the packet.
		conn := tls.Server(c.conn, l.TLSConfig)
		c.conn = conn
		c.reader.Reset(conn)
		c.writer.Reset(conn)
		c.Capabilities |= CapabilityClientSSL
		return "", "", nil, nil
	}

	// username
	username, pos, ok := readNullString(data, pos)
	if !ok {
		return "", "", nil, fmt.Errorf("parseClientHandshakePacket: can't read username")
	}

	// auth-response can have three forms.
	var authResponse []byte
	if clientFlags&CapabilityClientPluginAuthLenencClientData != 0 {
		var l uint64
		l, pos, ok = readLenEncInt(data, pos)
		if !ok {
			return "", "", nil, fmt.Errorf("parseClientHandshakePacket: can't read auth-response variable length")
		}
		authResponse, pos, ok = readBytesCopy(data, pos, int(l))
		if !ok {
			return "", "", nil, fmt.Errorf("parseClientHandshakePacket: can't read auth-response")
		}

	} else if clientFlags&CapabilityClientSecureConnection != 0 {
		var l byte
		l, pos, ok = readByte(data, pos)
		if !ok {
			return "", "", nil, fmt.Errorf("parseClientHandshakePacket: can't read auth-response length")
		}

		authResponse, pos, ok = readBytesCopy(data, pos, int(l))
		if !ok {
			return "", "", nil, fmt.Errorf("parseClientHandshakePacket: can't read auth-response")
		}
	} else {
		a := ""
		a, pos, ok = readNullString(data, pos)
		if !ok {
			return "", "", nil, fmt.Errorf("parseClientHandshakePacket: can't read auth-response")
		}
		authResponse = []byte(a)
	}

	// db name.
	if clientFlags&CapabilityClientConnectWithDB != 0 {
		dbname := ""
		dbname, pos, ok = readNullString(data, pos)
		if !ok {
			return "", "", nil, fmt.Errorf("parseClientHandshakePacket: can't read dbname")
		}
		c.SchemaName = dbname
	}

	// authMethod (with default)
	authMethod := MysqlNativePassword
	if clientFlags&CapabilityClientPluginAuth != 0 {
		authMethod, pos, ok = readNullString(data, pos)
		if !ok {
			return "", "", nil, fmt.Errorf("parseClientHandshakePacket: can't read authMethod")
		}
	}

	// FIXME(alainjobart) Add CLIENT_CONNECT_ATTRS parsing if we need it.

	return username, authMethod, authResponse, nil
}

// writeAuthSwitchRequest writes an auth switch request packet.
func (c *Conn) writeAuthSwitchRequest(pluginName string, pluginData []byte) error {
	length := 1 + // AuthSwitchRequestPacket
		len(pluginName) + 1 + // 0-terminated pluginName
		len(pluginData)

	data := c.startEphemeralPacket(length)
	pos := 0

	// Packet header.
	pos = writeByte(data, pos, AuthSwitchRequestPacket)

	// Copy server version.
	pos = writeNullString(data, pos, pluginName)

	// Copy auth data.
	pos += copy(data[pos:], pluginData)

	// Sanity check.
	if pos != len(data) {
		return fmt.Errorf("error building AuthSwitchRequestPacket packet: got %v bytes expected %v", pos, len(data))
	}
	if err := c.writeEphemeralPacket(true); err != nil {
		return err
	}
	return nil
}
