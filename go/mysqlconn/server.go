package mysqlconn

import (
	"bytes"
	"crypto/rand"
	"fmt"
	"net"

	log "github.com/golang/glog"
	"github.com/youtube/vitess/go/sqltypes"
	"github.com/youtube/vitess/go/tb"
)

const (
	// DefaultServerVersion is the default server version we're sending to the client.
	// Can be changed.
	DefaultServerVersion = "5.5.10-Vitess"
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
	ComQuery(c *Conn, query string) (*sqltypes.Result, error)
}

// Listener is the MySQL server protocol listener.
type Listener struct {
	// Construction parameters, set by NewListener.

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

	// PasswordMap maps users to passwords.
	PasswordMap map[string]string

	// The following parameters are changed by the Accept routine.

	// Incrementing ID for connection id.
	connectionID uint32
}

// NewListener creates a new Listener.
func NewListener(protocol, address string, handler Handler) (*Listener, error) {
	listener, err := net.Listen(protocol, address)
	if err != nil {
		return nil, err
	}

	return &Listener{
		ServerVersion: DefaultServerVersion,
		handler:       handler,
		PasswordMap:   make(map[string]string),
		listener:      listener,
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

		connectionID := l.connectionID
		l.connectionID++

		go l.handle(conn, connectionID)
	}
}

// handle is called in a go routine for each client connection.
// FIXME(alainjobart) handle per-connection logs in a way that makes sense.
// FIXME(alainjobart) add an idle timeout for the connection.
func (l *Listener) handle(conn net.Conn, connectionID uint32) {
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

	// First build and send the server handshake packet.
	cipher, err := c.writeHandshakeV10(l.ServerVersion)
	if err != nil {
		log.Errorf("Cannot send HandshakeV10 packet: %v", err)
		return
	}

	// Wait for the client response.
	response, err := c.readEphemeralPacket()
	if err != nil {
		log.Errorf("Cannot read client handshake response: %v", err)
		return
	}
	username, authResponse, err := l.parseClientHandshakePacket(c, response)
	if err != nil {
		log.Errorf("Cannot parse client handshake response: %v", err)
		return
	}

	// Find the user in our map
	password, ok := l.PasswordMap[username]
	if !ok {
		log.Errorf("Invalid user: %v", username)
		c.writeErrorPacket(ERAccessDeniedError, SSAccessDeniedError, "Access denied for user '%v'", username)
		return
	}

	// Validate the password.
	computedAuthResponse := scramblePassword(cipher, []byte(password))
	if bytes.Compare(authResponse, computedAuthResponse) != 0 {
		log.Errorf("Invalid password for user %v", username)
		c.writeErrorPacket(ERAccessDeniedError, SSAccessDeniedError, "Access denied for user '%v'", username)
		return
	}

	// Send an OK packet.
	if err := c.writeOKPacket(0, 0, c.StatusFlags, 0); err != nil {
		log.Errorf("Cannot write OK packet: %v", err)
		return
	}

	for {
		c.sequence = 0
		data, err := c.readEphemeralPacket()
		if err != nil {
			log.Errorf("Error reading packet from client %v: %v", c.ConnectionID, err)
			return
		}

		switch data[0] {
		case ComQuit:
			log.Infof("Client %v wants to disconnect, closing connection.", c.ConnectionID)
			return
		case ComInitDB:
			db := c.parseComInitDB(data)
			c.SchemaName = db
			if err := c.writeOKPacket(0, 0, c.StatusFlags, 0); err != nil {
				log.Errorf("Error writing ComInitDB result to client %v: %v", c.ConnectionID, err)
				return
			}
		case ComQuery:
			query := c.parseComQuery(data)
			log.Infof("Received command from client %v: %v", c.ConnectionID, query)
			result, err := l.handler.ComQuery(c, query)
			if err != nil {
				if werr := c.writeErrorPacketFromError(err); werr != nil {
					// If we can't even write the error, we're done.
					log.Errorf("Error writing query error to client %v: %v", c.ConnectionID, werr)
					return
				}
				continue
			}
			if err := c.writeResult(result); err != nil {
				log.Errorf("Error writing result to client %v: %v", c.ConnectionID, err)
				return
			}
		case ComPing:
			// No payload to that one, just return OKPacket.
			if err := c.writeOKPacket(0, 0, c.StatusFlags, 0); err != nil {
				log.Errorf("Error writing ComPing result to client %v: %v", c.ConnectionID, err)
				return
			}
		default:
			log.Errorf("Got unhandled packet from client %v, returning error: %v", c.ConnectionID, data)
			if err := c.writeErrorPacket(ERUnknownComError, SSUnknownComError, "command handling not implemented yet: %v", data[0]); err != nil {
				log.Errorf("Error writing error packet to client: %v", err)
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
// It returns the cipher data.
func (c *Conn) writeHandshakeV10(serverVersion string) ([]byte, error) {
	capabilities := CapabilityClientLongPassword |
		CapabilityClientLongFlag |
		CapabilityClientConnectWithDB |
		CapabilityClientProtocol41 |
		CapabilityClientTransactions |
		CapabilityClientSecureConnection |
		CapabilityClientPluginAuth |
		CapabilityClientPluginAuthLenencClientData |
		CapabilityClientDeprecateEOF

	length :=
		1 + // protocol version
			lenNullString(serverVersion) +
			4 + // connection ID
			8 + // first part of cipher data
			1 + // filler byte
			2 + // capability flags (lower 2 bytes)
			1 + // character set
			2 + // status flag
			2 + // capability flags (upper 2 bytes)
			1 + // length of auth plugin data
			10 + // reserved (0)
			13 + // auth-plugin-data
			lenNullString(mysqlNativePassword) // auth-plugin-name

	data := c.startEphemeralPacket(length)
	pos := 0

	// Protocol version.
	pos = writeByte(data, pos, protocolVersion)

	// Copy server version.
	pos = writeNullString(data, pos, serverVersion)

	// Add connectionID in.
	pos = writeUint32(data, pos, c.ConnectionID)

	// Generate the cipher, put 8 bytes in.
	cipher := make([]byte, 20)
	if _, err := rand.Read(cipher); err != nil {
		return nil, err
	}

        // Cipher must be a legal UTF8 string.
	for i := 0; i < len(cipher); i++ {
		cipher[i] &= 0x7f
		if cipher[i] == '\x00' || cipher[i] == '$' {
			cipher[i] += 1
		}
	}
	pos += copy(data[pos:], cipher[:8])

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
	pos += copy(data[pos:], cipher[8:])
	data[pos] = 0
	pos++

	// Copy authPluginName.
	pos = writeNullString(data, pos, mysqlNativePassword)

	// Sanity check.
	if pos != len(data) {
		return nil, fmt.Errorf("error building Handshake packet: got %v bytes expected %v", pos, len(data))
	}

	if err := c.writeEphemeralPacket(true); err != nil {
		return nil, err
	}

	return cipher, nil
}

// parseClientHandshakePacket parses the handshake sent by the client.
// Returns the username, auth-data, error.
func (l *Listener) parseClientHandshakePacket(c *Conn, data []byte) (string, []byte, error) {
	pos := 0

	// Client flags, 4 bytes.
	clientFlags, pos, ok := readUint32(data, pos)
	if !ok {
		return "", nil, fmt.Errorf("parseClientHandshakePacket: can't read client flags")
	}
	if clientFlags&CapabilityClientProtocol41 == 0 {
		return "", nil, fmt.Errorf("parseClientHandshakePacket: only support protocol 4.1")
	}

	// Remember a subset of the capabilities, so we can use them later in the protocol.
	c.Capabilities = clientFlags & (CapabilityClientDeprecateEOF)

	// Max packet size. Don't do anything with this now.
	// See doc.go for more information.
	/*maxPacketSize*/
	_, pos, ok = readUint32(data, pos)
	if !ok {
		return "", nil, fmt.Errorf("parseClientHandshakePacket: can't read maxPacketSize")
	}

	// Character set. Need to handle it.
	characterSet, pos, ok := readByte(data, pos)
	if !ok {
		return "", nil, fmt.Errorf("parseClientHandshakePacket: can't read characterSet")
	}
	c.CharacterSet = characterSet

	// 23x reserved zero bytes.
	pos += 23

	// username
	username, pos, ok := readNullString(data, pos)
	if !ok {
		return "", nil, fmt.Errorf("parseClientHandshakePacket: can't read username")
	}

	// auth-response can have three forms.
	var authResponse []byte
	if clientFlags&CapabilityClientPluginAuthLenencClientData != 0 {
		var l uint64
		l, pos, ok = readLenEncInt(data, pos)
		if !ok {
			return "", nil, fmt.Errorf("parseClientHandshakePacket: can't read auth-response variable length")
		}
		authResponse, pos, ok = readBytes(data, pos, int(l))
		if !ok {
			return "", nil, fmt.Errorf("parseClientHandshakePacket: can't read auth-response")
		}

	} else if clientFlags&CapabilityClientSecureConnection != 0 {
		var l byte
		l, pos, ok = readByte(data, pos)
		if !ok {
			return "", nil, fmt.Errorf("parseClientHandshakePacket: can't read auth-response length")
		}

		authResponse, pos, ok = readBytes(data, pos, int(l))
		if !ok {
			return "", nil, fmt.Errorf("parseClientHandshakePacket: can't read auth-response")
		}
	} else {
		a := ""
		a, pos, ok = readNullString(data, pos)
		if !ok {
			return "", nil, fmt.Errorf("parseClientHandshakePacket: can't read auth-response")
		}
		authResponse = []byte(a)
	}

	// db name.
	if clientFlags&CapabilityClientConnectWithDB != 0 {
		dbname := ""
		dbname, pos, ok = readNullString(data, pos)
		if !ok {
			return "", nil, fmt.Errorf("parseClientHandshakePacket: can't read dbname")
		}
		c.SchemaName = dbname
	}

	// auth plugin name
	authPluginName := "mysql_native_password"
	if clientFlags&CapabilityClientPluginAuth != 0 {
		authPluginName, pos, ok = readNullString(data, pos)
		if !ok {
			return "", nil, fmt.Errorf("parseClientHandshakePacket: can't read authPluginName")
		}
	}
	if authPluginName != mysqlNativePassword {
		return "", nil, fmt.Errorf("invalid authPluginName, got %v but only support %v", authPluginName, mysqlNativePassword)
	}

	// FIXME(alainjobart) Add CLIENT_CONNECT_ATTRS parsing if we need it.

	return username, authResponse, nil
}
