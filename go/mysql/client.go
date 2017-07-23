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
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"

	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/vt/servenv/grpcutils"
)

// connectResult is used by Connect.
type connectResult struct {
	c   *Conn
	err error
}

// Connect creates a connection to a server.
// It then handles the initial handshake.
//
// If context is canceled before the end of the process, this function
// will return nil, ctx.Err().
//
// FIXME(alainjobart) once we have more of a server side, add test cases
// to cover all failure scenarios.
func Connect(ctx context.Context, params *ConnParams) (*Conn, error) {
	netProto := "tcp"
	addr := ""
	if params.UnixSocket != "" {
		netProto = "unix"
		addr = params.UnixSocket
	} else {
		addr = net.JoinHostPort(params.Host, fmt.Sprintf("%v", params.Port))
	}

	// Figure out the character set we want.
	characterSet, err := parseCharacterSet(params.Charset)
	if err != nil {
		return nil, err
	}

	// Start a background connection routine.  It first
	// establishes a network connection, returns it on the channel,
	// then starts the negotiation, and returns the result on the channel.
	// It can send on the channel, before closing it:
	// - a connectResult with an error and nothing else (when dial fails).
	// - a connectResult with a *Conn and no error, then another one
	//   with possibly an error.
	status := make(chan connectResult)
	go func() {
		defer close(status)
		var err error
		var conn net.Conn

		// Cap the Dial time with the context deadline, plus a
		// few seconds. We want to reclaim resources quickly
		// and not let this go routine stuck in Dial forever.
		//
		// We add a few seconds so we detect the context is
		// Done() before timing out the Dial. That way we'll
		// return the right error to the client (ctx.Err(), vs
		// DialTimeout() error).
		if deadline, ok := ctx.Deadline(); ok {
			timeout := deadline.Sub(time.Now()) + 5*time.Second
			conn, err = net.DialTimeout(netProto, addr, timeout)
		} else {
			conn, err = net.Dial(netProto, addr)
		}
		if err != nil {
			// If we get an error, the connection to a Unix socket
			// should return a 2002, but for a TCP socket it
			// should return a 2003.
			if netProto == "tcp" {
				status <- connectResult{
					err: NewSQLError(CRConnHostError, SSUnknownSQLState, "net.Dial(%v) failed: %v", addr, err),
				}
			} else {
				status <- connectResult{
					err: NewSQLError(CRConnectionError, SSUnknownSQLState, "net.Dial(%v) to local server failed: %v", addr, err),
				}
			}
			return
		}

		// Send the connection back, so the other side can close it.
		c := newConn(conn)
		status <- connectResult{
			c: c,
		}

		// During the handshake, and if the context is
		// canceled, the connection will be closed. That will
		// make any read or write just return with an error
		// right away.
		status <- connectResult{
			err: c.clientHandshake(characterSet, params),
		}
	}()

	// Wait on the context and the status, for the connection to happen.
	var c *Conn
	select {
	case <-ctx.Done():
		// The background routine may send us a few things,
		// wait for them and terminate them properly in the
		// background.
		go func() {
			dialCR := <-status // This one can take a while.
			if dialCR.err != nil {
				// Dial failed, nothing else to do.
				return
			}
			// Dial worked, close the connection, wait for the end.
			// We wait as not to leave a channel with an unread value.
			dialCR.c.Close()
			<-status
		}()
		return nil, ctx.Err()
	case cr := <-status:
		if cr.err != nil {
			// Dial failed, no connection was ever established.
			return nil, cr.err
		}

		// Dial worked, we have a connection. Keep going.
		c = cr.c
	}

	// Wait for the end of the handshake.
	select {
	case <-ctx.Done():
		// We are interrupted. Close the connection, wait for
		// the handshake to finish in the background.
		c.Close()
		go func() {
			// Since we closed the connection, this one should be fast.
			// We wait as not to leave a channel with an unread value.
			<-status
		}()
		return nil, ctx.Err()
	case cr := <-status:
		if cr.err != nil {
			c.Close()
			return nil, cr.err
		}
	}
	return c, nil
}

// parseCharacterSet parses the provided character set.
// Returns SQLError(CRCantReadCharset) if it can't.
func parseCharacterSet(cs string) (uint8, error) {
	// Check if it's empty, return utf8. This is a reasonable default.
	if cs == "" {
		return CharacterSetUtf8, nil
	}

	// Check if it's in our map.
	characterSet, ok := CharacterSetMap[strings.ToLower(cs)]
	if ok {
		return characterSet, nil
	}

	// As a fallback, try to parse a number. So we support more values.
	if i, err := strconv.ParseInt(cs, 10, 8); err == nil {
		return uint8(i), nil
	}

	// No luck.
	return 0, NewSQLError(CRCantReadCharset, SSUnknownSQLState, "failed to interpret character set '%v'. Try using an integer value if needed", cs)
}

// clientHandshake handles the client side of the handshake.
// Note the connection can be closed while this is running.
// Returns a SQLError.
func (c *Conn) clientHandshake(characterSet uint8, params *ConnParams) error {
	// Wait for the server initial handshake packet, and parse it.
	data, err := c.readPacket()
	if err != nil {
		return NewSQLError(CRServerLost, "", "initial packet read failed: %v", err)
	}
	capabilities, salt, err := c.parseInitialHandshakePacket(data)
	if err != nil {
		return err
	}

	// Sanity check.
	if capabilities&CapabilityClientProtocol41 == 0 {
		return NewSQLError(CRVersionError, SSUnknownSQLState, "cannot connect to servers earlier than 4.1")
	}

	// Remember a subset of the capabilities, so we can use them
	// later in the protocol.
	c.Capabilities = capabilities & (CapabilityClientDeprecateEOF)

	// Handle switch to SSL if necessary.
	if params.Flags&CapabilityClientSSL > 0 {
		// If client asked for SSL, but server doesn't support it,
		// stop right here.
		if capabilities&CapabilityClientSSL == 0 {
			return NewSQLError(CRSSLConnectionError, SSUnknownSQLState, "server doesn't support SSL but client asked for it")
		}

		// The ServerName to verify depends on what the hostname is.
		// - If using a socket, we use "localhost".
		// - If it is an IP address, we need to prefix it with 'IP:'.
		// - If not, we can just use it as is.
		// We may need to add a ServerName field to ConnParams to
		// make this more explicit.
		serverName := "localhost"
		if params.Host != "" {
			if net.ParseIP(params.Host) != nil {
				serverName = "IP:" + params.Host
			} else {
				serverName = params.Host
			}
		}

		// Build the TLS config.
		clientConfig, err := grpcutils.TLSClientConfig(params.SslCert, params.SslKey, params.SslCa, serverName)
		if err != nil {
			return NewSQLError(CRSSLConnectionError, SSUnknownSQLState, "error loading client cert and ca: %v", err)
		}

		// Send the SSLRequest packet.
		if err := c.writeSSLRequest(capabilities, characterSet, params); err != nil {
			return err
		}

		// Switch to SSL.
		conn := tls.Client(c.conn, clientConfig)
		c.conn = conn
		c.reader.Reset(conn)
		c.writer.Reset(conn)
		c.Capabilities |= CapabilityClientSSL
	}

	// Password encryption.
	scrambledPassword := scramblePassword(salt, []byte(params.Pass))

	// Build and send our handshake response 41.
	// Note this one will never have SSL flag on.
	if err := c.writeHandshakeResponse41(capabilities, scrambledPassword, characterSet, params); err != nil {
		return err
	}

	// Read the server response.
	response, err := c.readPacket()
	if err != nil {
		return NewSQLError(CRServerLost, SSUnknownSQLState, "%v", err)
	}
	switch response[0] {
	case OKPacket:
		// OK packet, we are authenticated. Save the user, keep going.
		c.User = params.Uname
	case AuthSwitchRequestPacket:
		// Server is asking to use a different auth method. We
		// only support cleartext plugin.
		pluginName, _, err := parseAuthSwitchRequest(response)
		if err != nil {
			return NewSQLError(CRServerHandshakeErr, SSUnknownSQLState, "cannot parse auth switch request: %v", err)
		}
		if pluginName != MysqlClearPassword {
			return NewSQLError(CRServerHandshakeErr, SSUnknownSQLState, "server asked for unsupported auth method: %v", pluginName)
		}

		// Write the password packet.
		if err := c.writeClearTextPassword(params); err != nil {
			return err
		}

		// Wait for OK packet.
		response, err = c.readPacket()
		if err != nil {
			return NewSQLError(CRServerLost, SSUnknownSQLState, "%v", err)
		}
		switch response[0] {
		case OKPacket:
			// OK packet, we are authenticated. Save the user, keep going.
			c.User = params.Uname
		case ErrPacket:
			return ParseErrorPacket(response)
		default:
			return NewSQLError(CRServerHandshakeErr, SSUnknownSQLState, "initial server response cannot be parsed: %v", response)
		}
	case ErrPacket:
		return ParseErrorPacket(response)
	default:
		return NewSQLError(CRServerHandshakeErr, SSUnknownSQLState, "initial server response cannot be parsed: %v", response)
	}

	// If the server didn't support DbName in its handshake, set
	// it now. This is what the 'mysql' client does.
	if capabilities&CapabilityClientConnectWithDB == 0 && params.DbName != "" {
		// Write the packet.
		if err := c.writeComInitDB(params.DbName); err != nil {
			return err
		}

		// Wait for response, should be OK.
		response, err := c.readPacket()
		if err != nil {
			return NewSQLError(CRServerLost, SSUnknownSQLState, "%v", err)
		}
		switch response[0] {
		case OKPacket:
			// OK packet, we are authenticated.
			return nil
		case ErrPacket:
			return ParseErrorPacket(response)
		default:
			// FIXME(alainjobart) handle extra auth cases and so on.
			return NewSQLError(CRServerHandshakeErr, SSUnknownSQLState, "initial server response is asking for more information, not implemented yet: %v", response)
		}
	}

	return nil
}

// parseInitialHandshakePacket parses the initial handshake from the server.
// It returns a SQLError with the right code.
func (c *Conn) parseInitialHandshakePacket(data []byte) (uint32, []byte, error) {
	pos := 0

	// Protocol version.
	pver, pos, ok := readByte(data, pos)
	if !ok {
		return 0, nil, NewSQLError(CRVersionError, SSUnknownSQLState, "parseInitialHandshakePacket: packet has no protocol version")
	}
	if pver != protocolVersion {
		return 0, nil, NewSQLError(CRVersionError, SSUnknownSQLState, "bad protocol version: %v", pver)
	}

	// Read the server version.
	c.ServerVersion, pos, ok = readNullString(data, pos)
	if !ok {
		return 0, nil, NewSQLError(CRMalformedPacket, SSUnknownSQLState, "parseInitialHandshakePacket: packet has no server version")
	}
	c.fillFlavor()

	// Read the connection id.
	c.ConnectionID, pos, ok = readUint32(data, pos)
	if !ok {
		return 0, nil, NewSQLError(CRMalformedPacket, SSUnknownSQLState, "parseInitialHandshakePacket: packet has no conneciton id")
	}

	// Read the first part of the auth-plugin-data
	authPluginData, pos, ok := readBytes(data, pos, 8)
	if !ok {
		return 0, nil, NewSQLError(CRMalformedPacket, SSUnknownSQLState, "parseInitialHandshakePacket: packet has no auth-plugin-data-part-1")
	}

	// One byte filler, 0. We don't really care about the value.
	_, pos, ok = readByte(data, pos)
	if !ok {
		return 0, nil, NewSQLError(CRMalformedPacket, SSUnknownSQLState, "parseInitialHandshakePacket: packet has no filler")
	}

	// Lower 2 bytes of the capability flags.
	capLower, pos, ok := readUint16(data, pos)
	if !ok {
		return 0, nil, NewSQLError(CRMalformedPacket, SSUnknownSQLState, "parseInitialHandshakePacket: packet has no capability flags (lower 2 bytes)")
	}
	var capabilities = uint32(capLower)

	// The packet can end here.
	if pos == len(data) {
		return capabilities, authPluginData, nil
	}

	// Character set.
	characterSet, pos, ok := readByte(data, pos)
	if !ok {
		return 0, nil, NewSQLError(CRMalformedPacket, SSUnknownSQLState, "parseInitialHandshakePacket: packet has no character set")
	}
	c.CharacterSet = characterSet

	// Status flags. Ignored.
	_, pos, ok = readUint16(data, pos)
	if !ok {
		return 0, nil, NewSQLError(CRMalformedPacket, SSUnknownSQLState, "parseInitialHandshakePacket: packet has no status flags")
	}

	// Upper 2 bytes of the capability flags.
	capUpper, pos, ok := readUint16(data, pos)
	if !ok {
		return 0, nil, NewSQLError(CRMalformedPacket, SSUnknownSQLState, "parseInitialHandshakePacket: packet has no capability flags (upper 2 bytes)")
	}
	capabilities += uint32(capUpper) << 16

	// Length of auth-plugin-data, or 0.
	// Only with CLIENT_PLUGIN_AUTH capability.
	var authPluginDataLength byte
	if capabilities&CapabilityClientPluginAuth != 0 {
		authPluginDataLength, pos, ok = readByte(data, pos)
		if !ok {
			return 0, nil, NewSQLError(CRMalformedPacket, SSUnknownSQLState, "parseInitialHandshakePacket: packet has no length of auth-plugin-data")
		}
	} else {
		// One byte filler, 0. We don't really care about the value.
		_, pos, ok = readByte(data, pos)
		if !ok {
			return 0, nil, NewSQLError(CRMalformedPacket, SSUnknownSQLState, "parseInitialHandshakePacket: packet has no length of auth-plugin-data filler")
		}
	}

	// 10 reserved 0 bytes.
	pos += 10

	if capabilities&CapabilityClientSecureConnection != 0 {
		// The next part of the auth-plugin-data.
		// The length is max(13, length of auth-plugin-data - 8).
		l := int(authPluginDataLength) - 8
		if l > 13 {
			l = 13
		}
		var authPluginDataPart2 []byte
		authPluginDataPart2, pos, ok = readBytes(data, pos, l)
		if !ok {
			return 0, nil, NewSQLError(CRMalformedPacket, SSUnknownSQLState, "parseInitialHandshakePacket: packet has no auth-plugin-data-part-2")
		}

		// The last byte has to be 0, and is not part of the data.
		if authPluginDataPart2[l-1] != 0 {
			return 0, nil, NewSQLError(CRMalformedPacket, SSUnknownSQLState, "parseInitialHandshakePacket: auth-plugin-data-part-2 is not 0 terminated")
		}
		authPluginData = append(authPluginData, authPluginDataPart2[0:l-1]...)
	}

	// Auth-plugin name.
	if capabilities&CapabilityClientPluginAuth != 0 {
		authPluginName, _, ok := readNullString(data, pos)
		if !ok {
			// Fallback for versions prior to 5.5.10 and
			// 5.6.2 that don't have a null terminated string.
			authPluginName = string(data[pos : len(data)-1])
		}

		if authPluginName != MysqlNativePassword {
			return 0, nil, NewSQLError(CRMalformedPacket, SSUnknownSQLState, "parseInitialHandshakePacket: only support %v auth plugin name, but got %v", MysqlNativePassword, authPluginName)
		}
	}

	return capabilities, authPluginData, nil
}

// writeSSLRequest writes the SSLRequest packet. It's just a truncated
// HandshakeResponse41.
func (c *Conn) writeSSLRequest(capabilities uint32, characterSet uint8, params *ConnParams) error {
	// Build our flags, with CapabilityClientSSL.
	var flags uint32 = CapabilityClientLongPassword |
		CapabilityClientLongFlag |
		CapabilityClientProtocol41 |
		CapabilityClientTransactions |
		CapabilityClientSecureConnection |
		CapabilityClientPluginAuth |
		CapabilityClientPluginAuthLenencClientData |
		CapabilityClientSSL |
		// If the server supported
		// CapabilityClientDeprecateEOF, we also support it.
		c.Capabilities&CapabilityClientDeprecateEOF |
		// Pass-through ClientFoundRows flag.
		CapabilityClientFoundRows&uint32(params.Flags)

	length :=
		4 + // Client capability flags.
			4 + // Max-packet size.
			1 + // Character set.
			23 // Reserved.

	// Add the DB name if the server supports it.
	if params.DbName != "" && (capabilities&CapabilityClientConnectWithDB != 0) {
		flags |= CapabilityClientConnectWithDB
	}

	data := c.startEphemeralPacket(length)
	pos := 0

	// Client capability flags.
	pos = writeUint32(data, pos, flags)

	// Max-packet size, always 0. See doc.go.
	pos = writeZeroes(data, pos, 4)

	// Character set.
	pos = writeByte(data, pos, characterSet)

	// And send it as is.
	if err := c.writeEphemeralPacket(true /* direct */); err != nil {
		return NewSQLError(CRServerLost, SSUnknownSQLState, "cannot send SSLRequest: %v", err)
	}
	return nil
}

// writeHandshakeResponse41 writes the handshake response.
// Returns a SQLError.
func (c *Conn) writeHandshakeResponse41(capabilities uint32, scrambledPassword []byte, characterSet uint8, params *ConnParams) error {
	// Build our flags.
	var flags uint32 = CapabilityClientLongPassword |
		CapabilityClientLongFlag |
		CapabilityClientProtocol41 |
		CapabilityClientTransactions |
		CapabilityClientSecureConnection |
		CapabilityClientPluginAuth |
		CapabilityClientPluginAuthLenencClientData |
		// If the server supported
		// CapabilityClientDeprecateEOF, we also support it.
		c.Capabilities&CapabilityClientDeprecateEOF |
		// Pass-through ClientFoundRows flag.
		CapabilityClientFoundRows&uint32(params.Flags)

	// FIXME(alainjobart) add multi statement.

	length :=
		4 + // Client capability flags.
			4 + // Max-packet size.
			1 + // Character set.
			23 + // Reserved.
			lenNullString(params.Uname) +
			// length of scrambled passsword is handled below.
			len(scrambledPassword) +
			21 + // "mysql_native_password" string.
			1 // terminating zero.

	// Add the DB name if the server supports it.
	if params.DbName != "" && (capabilities&CapabilityClientConnectWithDB != 0) {
		flags |= CapabilityClientConnectWithDB
		length += lenNullString(params.DbName)
	}

	if capabilities&CapabilityClientPluginAuthLenencClientData != 0 {
		length += lenEncIntSize(uint64(len(scrambledPassword)))
	} else {
		length++
	}

	data := c.startEphemeralPacket(length)
	pos := 0

	// Client capability flags.
	pos = writeUint32(data, pos, flags)

	// Max-packet size, always 0. See doc.go.
	pos = writeZeroes(data, pos, 4)

	// Character set.
	pos = writeByte(data, pos, characterSet)

	// 23 reserved bytes, all 0.
	pos = writeZeroes(data, pos, 23)

	// Username
	pos = writeNullString(data, pos, params.Uname)

	// Scrambled password.  The length is encoded as variable length if
	// CapabilityClientPluginAuthLenencClientData is set.
	if capabilities&CapabilityClientPluginAuthLenencClientData != 0 {
		pos = writeLenEncInt(data, pos, uint64(len(scrambledPassword)))
	} else {
		data[pos] = byte(len(scrambledPassword))
		pos++
	}
	pos += copy(data[pos:], scrambledPassword)

	// DbName, only if server supports it.
	if params.DbName != "" && (capabilities&CapabilityClientConnectWithDB != 0) {
		pos = writeNullString(data, pos, params.DbName)
		c.SchemaName = params.DbName
	}

	// Assume native client during response
	pos = writeNullString(data, pos, MysqlNativePassword)

	// Sanity-check the length.
	if pos != len(data) {
		return NewSQLError(CRMalformedPacket, SSUnknownSQLState, "writeHandshakeResponse41: only packed %v bytes, out of %v allocated", pos, len(data))
	}

	if err := c.writeEphemeralPacket(true /* direct */); err != nil {
		return NewSQLError(CRServerLost, SSUnknownSQLState, "cannot send HandshakeResponse41: %v", err)
	}
	return nil
}

func parseAuthSwitchRequest(data []byte) (string, []byte, error) {
	pos := 1
	pluginName, pos, ok := readNullString(data, pos)
	if !ok {
		return "", nil, fmt.Errorf("cannot get plugin name from AuthSwitchRequest: %v", data)
	}

	return pluginName, data[pos:], nil
}

// writeClearTextPassword writes the clear text password.
// Returns a SQLError.
func (c *Conn) writeClearTextPassword(params *ConnParams) error {
	length := len(params.Pass) + 1
	data := c.startEphemeralPacket(length)
	pos := 0
	pos = writeNullString(data, pos, params.Pass)
	// Sanity check.
	if pos != len(data) {
		return fmt.Errorf("error building ClearTextPassword packet: got %v bytes expected %v", pos, len(data))
	}
	if err := c.writeEphemeralPacket(true); err != nil {
		return err
	}
	return nil
}
