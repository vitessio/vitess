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
	"vitess.io/vitess/go/sqltypes"
)

const (
	// MaxPacketSize is the maximum payload length of a packet
	// the server supports.
	MaxPacketSize = (1 << 24) - 1

	// protocolVersion is the current version of the protocol.
	// Always 10.
	protocolVersion = 10

	// https://dev.mysql.com/doc/refman/en/identifier-length.html
	MaxIdentifierLength = 64
)

// AuthMethodDescription is the type for different supported and
// implemented authentication methods.
type AuthMethodDescription string

// Supported auth forms.
const (
	// MysqlNativePassword uses a salt and transmits a hash on the wire.
	MysqlNativePassword = AuthMethodDescription("mysql_native_password")

	// MysqlClearPassword transmits the password in the clear.
	MysqlClearPassword = AuthMethodDescription("mysql_clear_password")

	// CachingSha2Password uses a salt and transmits a SHA256 hash on the wire.
	CachingSha2Password = AuthMethodDescription("caching_sha2_password")

	// MysqlDialog uses the dialog plugin on the client side.
	// It transmits data in the clear.
	MysqlDialog = AuthMethodDescription("dialog")
)

// Capability flags.
// Originally found in include/mysql/mysql_com.h
const (
	// CapabilityClientLongPassword is CLIENT_LONG_PASSWORD.
	// New more secure passwords. Assumed to be set since 4.1.1.
	// We do not check this anywhere.
	CapabilityClientLongPassword = 1

	// CapabilityClientFoundRows is CLIENT_FOUND_ROWS.
	CapabilityClientFoundRows = 1 << 1

	// CapabilityClientLongFlag is CLIENT_LONG_FLAG.
	// Longer flags in Protocol::ColumnDefinition320.
	// Set it everywhere, not used, as we use Protocol::ColumnDefinition41.
	CapabilityClientLongFlag = 1 << 2

	// CapabilityClientConnectWithDB is CLIENT_CONNECT_WITH_DB.
	// One can specify db on connect.
	CapabilityClientConnectWithDB = 1 << 3

	// CLIENT_NO_SCHEMA 1 << 4
	// Do not permit database.table.column. We do permit it.

	// CLIENT_COMPRESS 1 << 5
	// We do not support compression. CPU is usually our bottleneck.

	// CLIENT_ODBC 1 << 6
	// No special behavior since 3.22.

	// CLIENT_LOCAL_FILES 1 << 7
	// Client can use LOCAL INFILE request of LOAD DATA|XML.
	// We do not set it.

	// CLIENT_IGNORE_SPACE 1 << 8
	// Parser can ignore spaces before '('.
	// We ignore this.

	// CapabilityClientProtocol41 is CLIENT_PROTOCOL_41.
	// New 4.1 protocol. Enforced everywhere.
	CapabilityClientProtocol41 = 1 << 9

	// CLIENT_INTERACTIVE 1 << 10
	// Not specified, ignored.

	// CapabilityClientSSL is CLIENT_SSL.
	// Switch to SSL after handshake.
	CapabilityClientSSL = 1 << 11

	// CLIENT_IGNORE_SIGPIPE 1 << 12
	// Do not issue SIGPIPE if network failures occur (libmysqlclient only).

	// CapabilityClientTransactions is CLIENT_TRANSACTIONS.
	// Can send status flags in EOF_Packet.
	// This flag is optional in 3.23, but always set by the server since 4.0.
	// We just do it all the time.
	CapabilityClientTransactions = 1 << 13

	// CLIENT_RESERVED 1 << 14

	// CapabilityClientSecureConnection is CLIENT_SECURE_CONNECTION.
	// New 4.1 authentication. Always set, expected, never checked.
	CapabilityClientSecureConnection = 1 << 15

	// CapabilityClientMultiStatements is CLIENT_MULTI_STATEMENTS
	// Can handle multiple statements per COM_QUERY and COM_STMT_PREPARE.
	CapabilityClientMultiStatements = 1 << 16

	// CapabilityClientMultiResults is CLIENT_MULTI_RESULTS
	// Can send multiple resultsets for COM_QUERY.
	CapabilityClientMultiResults = 1 << 17

	// CapabilityClientPluginAuth is CLIENT_PLUGIN_AUTH.
	// Client supports plugin authentication.
	CapabilityClientPluginAuth = 1 << 19

	// CapabilityClientConnAttr is CLIENT_CONNECT_ATTRS
	// Permits connection attributes in Protocol::HandshakeResponse41.
	CapabilityClientConnAttr = 1 << 20

	// CapabilityClientPluginAuthLenencClientData is CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA
	CapabilityClientPluginAuthLenencClientData = 1 << 21

	// CLIENT_CAN_HANDLE_EXPIRED_PASSWORDS 1 << 22
	// Announces support for expired password extension.
	// Not yet supported.

	// CLIENT_SESSION_TRACK 1 << 23
	// Can set ServerSessionStateChanged in the Status Flags
	// and send session-state change data after a OK packet.
	// Not yet supported.
	CapabilityClientSessionTrack = 1 << 23

	// CapabilityClientDeprecateEOF is CLIENT_DEPRECATE_EOF
	// Expects an OK (instead of EOF) after the resultset rows of a Text Resultset.
	CapabilityClientDeprecateEOF = 1 << 24
)

// Status flags. They are returned by the server in a few cases.
// Originally found in include/mysql/mysql_com.h
// See http://dev.mysql.com/doc/internals/en/status-flags.html
const (
	// a transaction is active
	ServerStatusInTrans   uint16 = 0x0001
	NoServerStatusInTrans uint16 = 0xFFFE

	// auto-commit is enabled
	ServerStatusAutocommit   uint16 = 0x0002
	NoServerStatusAutocommit uint16 = 0xFFFD

	ServerMoreResultsExists     uint16 = 0x0008
	ServerStatusNoGoodIndexUsed uint16 = 0x0010
	ServerStatusNoIndexUsed     uint16 = 0x0020
	// Used by Binary Protocol Resultset to signal that COM_STMT_FETCH must be used to fetch the row-data.
	ServerStatusCursorExists       uint16 = 0x0040
	ServerStatusLastRowSent        uint16 = 0x0080
	ServerStatusDbDropped          uint16 = 0x0100
	ServerStatusNoBackslashEscapes uint16 = 0x0200
	ServerStatusMetadataChanged    uint16 = 0x0400
	ServerQueryWasSlow             uint16 = 0x0800
	ServerPsOutParams              uint16 = 0x1000
	// in a read-only transaction
	ServerStatusInTransReadonly uint16 = 0x2000
	// connection state information has changed
	ServerSessionStateChanged uint16 = 0x4000
)

// State Change Information
const (
	// one or more system variables changed.
	SessionTrackSystemVariables uint8 = 0x00
	// schema changed.
	SessionTrackSchema uint8 = 0x01
	// "track state change" changed.
	SessionTrackStateChange uint8 = 0x02
	// "track GTIDs" changed.
	SessionTrackGtids uint8 = 0x03
)

// Packet types.
// Originally found in include/mysql/mysql_com.h
const (
	// ComQuit is COM_QUIT.
	ComQuit = 0x01

	// ComInitDB is COM_INIT_DB.
	ComInitDB = 0x02

	// ComQuery is COM_QUERY.
	ComQuery = 0x03

	// ComFieldList is COM_Field_List.
	ComFieldList = 0x04

	// ComPing is COM_PING.
	ComPing = 0x0e

	// ComBinlogDump is COM_BINLOG_DUMP.
	ComBinlogDump = 0x12

	// ComSemiSyncAck is SEMI_SYNC_ACK.
	ComSemiSyncAck = 0xef

	// ComPrepare is COM_PREPARE.
	ComPrepare = 0x16

	// ComStmtExecute is COM_STMT_EXECUTE.
	ComStmtExecute = 0x17

	// ComStmtSendLongData is COM_STMT_SEND_LONG_DATA
	ComStmtSendLongData = 0x18

	// ComStmtClose is COM_STMT_CLOSE.
	ComStmtClose = 0x19

	// ComStmtReset is COM_STMT_RESET
	ComStmtReset = 0x1a

	//ComStmtFetch is COM_STMT_FETCH
	ComStmtFetch = 0x1c

	// ComSetOption is COM_SET_OPTION
	ComSetOption = 0x1b

	// ComResetConnection is COM_RESET_CONNECTION
	ComResetConnection = 0x1f

	// ComBinlogDumpGTID is COM_BINLOG_DUMP_GTID.
	ComBinlogDumpGTID = 0x1e

	// ComRegisterReplica is COM_REGISTER_SLAVE
	// https://dev.mysql.com/doc/internals/en/com-register-slave.html
	ComRegisterReplica = 0x15

	// OKPacket is the header of the OK packet.
	OKPacket = 0x00

	// EOFPacket is the header of the EOF packet.
	EOFPacket = 0xfe

	// ErrPacket is the header of the error packet.
	ErrPacket = 0xff

	// NullValue is the encoded value of NULL.
	NullValue = 0xfb
)

// Auth packet types
const (
	// AuthMoreDataPacket is sent when server requires more data to authenticate
	AuthMoreDataPacket = 0x01

	// CachingSha2FastAuth is sent before OKPacket when server authenticates using cache
	CachingSha2FastAuth = 0x03

	// CachingSha2FullAuth is sent when server requests un-scrambled password to authenticate
	CachingSha2FullAuth = 0x04

	// AuthSwitchRequestPacket is used to switch auth method.
	AuthSwitchRequestPacket = 0xfe
)

var typeInt24, _ = sqltypes.TypeToMySQL(sqltypes.Int24)
var typeTimestamp, _ = sqltypes.TypeToMySQL(sqltypes.Timestamp)
var typeYear, _ = sqltypes.TypeToMySQL(sqltypes.Year)
var typeNewDecimal, _ = sqltypes.TypeToMySQL(sqltypes.Decimal)

// IsNum returns true if a MySQL type is a numeric value.
// It is the same as IS_NUM defined in mysql.h.
func IsNum(typ uint8) bool {
	return (typ <= typeInt24 && typ != typeTimestamp) ||
		typ == typeYear ||
		typ == typeNewDecimal
}
