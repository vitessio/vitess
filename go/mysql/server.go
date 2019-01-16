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
	tls "crypto/tls"
	"fmt"
	"io"
	"net"
	"strings"
	"time"

	"vitess.io/vitess/go/netutil"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/stats"
	"vitess.io/vitess/go/sync2"
	"vitess.io/vitess/go/tb"
	"vitess.io/vitess/go/vt/log"
)

const (
	// DefaultServerVersion is the default server version we're sending to the client.
	// Can be changed.
	DefaultServerVersion = "5.5.10-Vitess"

	// timing metric keys
	connectTimingKey  = "Connect"
	queryTimingKey    = "Query"
	versionSSL30      = "SSL30"
	versionTLS10      = "TLS10"
	versionTLS11      = "TLS11"
	versionTLS12      = "TLS12"
	versionTLSUnknown = "UnknownTLSVersion"
	versionNoTLS      = "None"
)

var (
	// Metrics
	timings    = stats.NewTimings("MysqlServerTimings", "MySQL server timings", "operation")
	connCount  = stats.NewGauge("MysqlServerConnCount", "Active MySQL server connections")
	connAccept = stats.NewCounter("MysqlServerConnAccepted", "Connections accepted by MySQL server")
	connSlow   = stats.NewCounter("MysqlServerConnSlow", "Connections that took more than the configured mysql_slow_connect_warn_threshold to establish")

	connCountByTLSVer = stats.NewGaugesWithSingleLabel("MysqlServerConnCountByTLSVer", "Active MySQL server connections by TLS version", "tls")
	connCountPerUser  = stats.NewGaugesWithSingleLabel("MysqlServerConnCountPerUser", "Active MySQL server connections per user", "count")
	_                 = stats.NewGaugeFunc("MysqlServerConnCountUnauthenticated", "Active MySQL server connections that haven't authenticated yet", func() int64 {
		totalUsers := int64(0)
		for _, v := range connCountPerUser.Counts() {
			totalUsers += v
		}
		return connCount.Get() - totalUsers
	})
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
	ComQuery(c *Conn, query string, callback func(*sqltypes.Result) error) error

	// ComPrepare is called when a connection receives a prepared
	// statement query.
	ComPrepare(c *Conn, query string, callback func(*sqltypes.Result) error) error

	// ComStmtExecute is called when a connection receives a statement
	// execute query.
	ComStmtExecute(c *Conn, prepare *PrepareData, callback func(*sqltypes.Result) error) error

	// WarningCount is called at the end of each query to obtain
	// the value to be returned to the client in the EOF packet.
	// Note that this will be called either in the context of the
	// ComQuery callback if the result does not contain any fields,
	// or after the last ComQuery call completes.
	WarningCount(c *Conn) uint16
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

	// Read timeout on a given connection
	connReadTimeout time.Duration
	// Write timeout on a given connection
	connWriteTimeout time.Duration
	// connReadBufferSize is size of buffer for reads from underlying connection.
	// Reads are unbuffered if it's <=0.
	connReadBufferSize int

	// shutdown indicates that Shutdown method was called.
	shutdown sync2.AtomicBool

	// RequireSecureTransport configures the server to reject connections from insecure clients
	RequireSecureTransport bool
}

// NewFromListener creares a new mysql listener from an existing net.Listener
func NewFromListener(l net.Listener, authServer AuthServer, handler Handler, connReadTimeout time.Duration, connWriteTimeout time.Duration) (*Listener, error) {
	cfg := ListenerConfig{
		Listener:           l,
		AuthServer:         authServer,
		Handler:            handler,
		ConnReadTimeout:    connReadTimeout,
		ConnWriteTimeout:   connWriteTimeout,
		ConnReadBufferSize: connBufferSize,
	}
	return NewListenerWithConfig(cfg)
}

// NewListener creates a new Listener.
func NewListener(protocol, address string, authServer AuthServer, handler Handler, connReadTimeout time.Duration, connWriteTimeout time.Duration) (*Listener, error) {
	listener, err := net.Listen(protocol, address)
	if err != nil {
		return nil, err
	}

	return NewFromListener(listener, authServer, handler, connReadTimeout, connWriteTimeout)
}

// ListenerConfig should be used with NewListenerWithConfig to specify listener parameters.
type ListenerConfig struct {
	// Protocol-Address pair and Listener are mutually exclusive parameters
	Protocol           string
	Address            string
	Listener           net.Listener
	AuthServer         AuthServer
	Handler            Handler
	ConnReadTimeout    time.Duration
	ConnWriteTimeout   time.Duration
	ConnReadBufferSize int
}

// NewListenerWithConfig creates new listener using provided config. There are
// no default values for config, so caller should ensure its correctness.
func NewListenerWithConfig(cfg ListenerConfig) (*Listener, error) {
	var l net.Listener
	if cfg.Listener != nil {
		l = cfg.Listener
	} else {
		listener, err := net.Listen(cfg.Protocol, cfg.Address)
		if err != nil {
			return nil, err
		}
		l = listener
	}

	return &Listener{
		authServer:         cfg.AuthServer,
		handler:            cfg.Handler,
		listener:           l,
		ServerVersion:      DefaultServerVersion,
		connectionID:       1,
		connReadTimeout:    cfg.ConnReadTimeout,
		connWriteTimeout:   cfg.ConnWriteTimeout,
		connReadBufferSize: cfg.ConnReadBufferSize,
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
func (l *Listener) handle(conn net.Conn, connectionID uint32, acceptTime time.Time) {
	if l.connReadTimeout != 0 || l.connWriteTimeout != 0 {
		conn = netutil.NewConnWithTimeouts(conn, l.connReadTimeout, l.connWriteTimeout)
	}
	c := newServerConn(conn, l)
	c.ConnectionID = connectionID

	// Catch panics, and close the connection in any case.
	defer func() {
		if x := recover(); x != nil {
			log.Errorf("mysql_server caught panic:\n%v\n%s", x, tb.Stack(4))
		}
		// We call flush here in case there's a premature return after
		// startWriterBuffering is called
		c.flush()

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
		if err != io.EOF {
			log.Errorf("Cannot send HandshakeV10 packet to %s: %v", c, err)
		}
		return
	}

	// Wait for the client response. This has to be a direct read,
	// so we don't buffer the TLS negotiation packets.
	response, err := c.readEphemeralPacketDirect()
	if err != nil {
		// Don't log EOF errors. They cause too much spam, same as main read loop.
		if err != io.EOF {
			log.Errorf("Cannot read client handshake response from %s: %v", c, err)
		}
		return
	}
	user, authMethod, authResponse, err := l.parseClientHandshakePacket(c, true, response)
	if err != nil {
		log.Errorf("Cannot parse client handshake response from %s: %v", c, err)
		return
	}

	c.recycleReadPacket()

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

		if con, ok := c.conn.(*tls.Conn); ok {
			connState := con.ConnectionState()
			tlsVerStr := tlsVersionToString(connState.Version)
			if tlsVerStr != "" {
				connCountByTLSVer.Add(tlsVerStr, 1)
				defer connCountByTLSVer.Add(tlsVerStr, -1)
			}
		}
	} else {
		if l.RequireSecureTransport {
			c.writeErrorPacketFromError(fmt.Errorf("Server does not allow insecure connections, client must use SSL/TLS"))
		}
		connCountByTLSVer.Add(versionNoTLS, 1)
		defer connCountByTLSVer.Add(versionNoTLS, -1)
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
		// but the client returned a result for something else.

		salt, err := l.authServer.Salt()
		if err != nil {
			return
		}
		data := make([]byte, 21)
		data = append(salt, byte(0x00))
		if err := c.writeAuthSwitchRequest(MysqlNativePassword, data); err != nil {
			log.Errorf("Error writing auth switch packet for %s: %v", c, err)
			return
		}

		response, err := c.readEphemeralPacket()
		if err != nil {
			log.Errorf("Error reading auth switch response for %s: %v", c, err)
			return
		}
		c.recycleReadPacket()

		userData, err := l.authServer.ValidateHash(salt, user, response, conn.RemoteAddr())
		if err != nil {
			log.Warningf("Error authenticating user using MySQL native password: %v", err)
			c.writeErrorPacketFromError(err)
			return
		}
		c.User = user
		c.UserData = userData

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

	if c.User != "" {
		connCountPerUser.Add(c.User, 1)
		defer connCountPerUser.Add(c.User, -1)
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
		err := c.handleNextCommand(l.handler)
		if err != nil {
			return
		}
	}
}

// Close stops the listener, which prevents accept of any new connections. Existing connections won't be closed.
func (l *Listener) Close() {
	l.listener.Close()
}

// Shutdown closes listener and fails any Ping requests from existing connections.
// This can be used for graceful shutdown, to let clients know that they should reconnect to another server.
func (l *Listener) Shutdown() {
	if l.shutdown.CompareAndSwap(false, true) {
		l.Close()
	}
}

func (l *Listener) isShutdown() bool {
	return l.shutdown.Get()
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
		CapabilityClientMultiStatements |
		CapabilityClientMultiResults |
		CapabilityClientPluginAuth |
		CapabilityClientPluginAuthLenencClientData |
		CapabilityClientDeprecateEOF |
		CapabilityClientConnAttr
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

	// Reserved 10 bytes: all 0
	pos = writeZeroes(data, pos, 10)

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

	if err := c.writeEphemeralPacket(); err != nil {
		if strings.HasSuffix(err.Error(), "write: connection reset by peer") {
			return nil, io.EOF
		}
		if strings.HasSuffix(err.Error(), "write: broken pipe") {
			return nil, io.EOF
		}
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

	// set connection capability for executing multi statements
	if clientFlags&CapabilityClientMultiStatements > 0 {
		c.Capabilities |= CapabilityClientMultiStatements
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
		c.bufferedReader.Reset(conn)
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

	// The JDBC driver sometimes sends an empty string as the auth method when it wants to use mysql_native_password
	if authMethod == "" {
		authMethod = MysqlNativePassword
	}

	// Decode connection attributes send by the client
	if clientFlags&CapabilityClientConnAttr != 0 {
		var err error
		_, pos, err = parseConnAttrs(data, pos)
		if err != nil {
			return "", "", nil, err
		}
	}

	return username, authMethod, authResponse, nil
}

func parseConnAttrs(data []byte, pos int) (map[string]string, int, error) {
	var attrLen uint64

	attrLen, pos, ok := readLenEncInt(data, pos)
	if !ok {
		return nil, 0, fmt.Errorf("parseClientHandshakePacket: can't read connection attributes variable length")
	}

	var attrLenRead uint64

	attrs := make(map[string]string)

	for attrLenRead < attrLen {
		var keyLen byte
		keyLen, pos, ok = readByte(data, pos)
		if !ok {
			return nil, 0, fmt.Errorf("parseClientHandshakePacket: can't read connection attribute key length")
		}
		attrLenRead += uint64(keyLen) + 1

		var connAttrKey []byte
		connAttrKey, pos, ok = readBytesCopy(data, pos, int(keyLen))
		if !ok {
			return nil, 0, fmt.Errorf("parseClientHandshakePacket: can't read connection attribute key")
		}

		var valLen byte
		valLen, pos, ok = readByte(data, pos)
		if !ok {
			return nil, 0, fmt.Errorf("parseClientHandshakePacket: can't read connection attribute value length")
		}
		attrLenRead += uint64(valLen) + 1

		var connAttrVal []byte
		connAttrVal, pos, ok = readBytesCopy(data, pos, int(valLen))
		if !ok {
			return nil, 0, fmt.Errorf("parseClientHandshakePacket: can't read connection attribute value")
		}

		attrs[string(connAttrKey[:])] = string(connAttrVal[:])
	}

	return attrs, pos, nil

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
	return c.writeEphemeralPacket()
}

// Whenever we move to a new version of go, we will need add any new supported TLS versions here
func tlsVersionToString(version uint16) string {
	switch version {
	case tls.VersionSSL30:
		return versionSSL30
	case tls.VersionTLS10:
		return versionTLS10
	case tls.VersionTLS11:
		return versionTLS11
	case tls.VersionTLS12:
		return versionTLS12
	default:
		return versionTLSUnknown
	}
}
