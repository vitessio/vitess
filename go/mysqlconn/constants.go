package mysqlconn

const (
	// MaxPacketSize is the maximum payload length of a packet
	// the server supports.
	MaxPacketSize = (1 << 24) - 1

	// protocolVersion is the current version of the protocol.
	// Always 10.
	protocolVersion = 10

	// mysqlNativePassword is the auth form we use.
	mysqlNativePassword = "mysql_native_password"
)

// Capability flags.
// Originally found in include/mysql/mysql_com.h
const (
	// CapabilityClientLongPassword is CLIENT_LONG_PASSWORD.
	// New more secure passwords. Assumed to be set since 4.1.1.
	// We do not check this anywhere.
	CapabilityClientLongPassword = 1

	// CLIENT_FOUND_ROWS 1 << 1 See doc.go.

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
	// Not supported yet, but checked.
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

	// CLIENT_MULTI_STATEMENTS 1 << 16
	// Can handle multiple statements per COM_QUERY and COM_STMT_PREPARE.
	// Not yet supported.

	// CLIENT_MULTI_RESULTS 1 << 17
	// Can send multiple resultsets for COM_QUERY.
	// Not yet supported.

	// CLIENT_PS_MULTI_RESULTS 1 << 18
	// Can send multiple resultsets for COM_STMT_EXECUTE.
	// Not yet supported.

	// CapabilityClientPluginAuth is CLIENT_PLUGIN_AUTH.
	// Client supports plugin authentication.
	CapabilityClientPluginAuth = 1 << 19

	// CLIENT_CONNECT_ATTRS 1 << 20
	// Permits connection attributes in Protocol::HandshakeResponse41.
	// Not yet supported.

	// CapabilityClientPluginAuthLenencClientData is CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA
	CapabilityClientPluginAuthLenencClientData = 1 << 21

	// CLIENT_CAN_HANDLE_EXPIRED_PASSWORDS 1 << 22
	// Announces support for expired password extension.
	// Not yet supported.

	// CLIENT_SESSION_TRACK 1 << 23
	// Can set SERVER_SESSION_STATE_CHANGED in the Status Flags
	// and send session-state change data after a OK packet.
	// Not yet supported.

	// CapabilityClientDeprecateEOF is CLIENT_DEPRECATE_EOF
	// Expects an OK (instead of EOF) after the resultset rows of a Text Resultset.
	CapabilityClientDeprecateEOF = 1 << 24
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

	// ComPing is COM_PING.
	ComPing = 0x0e

	// ComBinlogDump is COM_BINLOG_DUMP.
	ComBinlogDump = 0x12

	// ComBinlogDumpGTID is COM_BINLOG_DUMP_GTID.
	ComBinlogDumpGTID = 0x1e

	// OKPacket is the header of the OK packet.
	OKPacket = 0x00

	// EOFPacket is the header of the EOF packet.
	EOFPacket = 0xfe

	// ErrPacket is the header of the error packet.
	ErrPacket = 0xff

	// NullValue is the encoded value of NULL.
	NullValue = 0xfb
)

// Error codes for client-side errors.
// Originally found in include/mysql/errmsg.h
const (
	// CRConnectionError is CR_CONNECTION_ERROR
	CRConnectionError = 2002

	// CRConnHostError is CR_CONN_HOST_ERROR
	CRConnHostError = 2003

	// CRServerHandshakeErr is CR_SERVER_HANDSHAKE_ERR
	CRServerHandshakeErr = 2012

	// CRServerLost is CR_SERVER_LOST
	CRServerLost = 2013
)

// Error codes for server-side errors.
// Originally found in include/mysql/mysqld_error.h
const (
	// ERAccessDeniedError is ER_ACCESS_DENIED_ERROR
	ERAccessDeniedError = 1045

	// ERUnknownComError is ER_UNKNOWN_COM_ERROR
	ERUnknownComError = 1047

	// ERDupEntry is ER_DUP_ENTRY
	ERDupEntry = 1062

	// ERUnknownError is ER_UNKNOWN_ERROR
	ERUnknownError = 1105

	// ERCantDoThisDuringAnTransaction is
	// ER_CANT_DO_THIS_DURING_AN_TRANSACTION
	ERCantDoThisDuringAnTransaction = 1179
)

// Sql states for errors.
// Originally found in include/mysql/sql_state.h
const (
	// SSSignalException is ER_SIGNAL_EXCEPTION
	SSSignalException = "HY000"

	// SSDupKey is ER_DUP_KEY
	SSDupKey = "23000"

	// SSAccessDeniedError is ER_ACCESS_DENIED_ERROR
	SSAccessDeniedError = "28000"

	// SSUnknownComError is ER_UNKNOWN_COM_ERROR
	SSUnknownComError = "08S01"

	// SSHandshakeError is ER_HANDSHAKE_ERROR
	SSHandshakeError = "08S01"

	// SSCantDoThisDuringAnTransaction is
	// ER_CANT_DO_THIS_DURING_AN_TRANSACTION
	SSCantDoThisDuringAnTransaction = "25000"
)

// Status flags. They are returned by the server in a few cases.
// Originally found in include/mysql/mysql_com.h
// See http://dev.mysql.com/doc/internals/en/status-flags.html
const (
	// ServerStatusAutocommit is SERVER_STATUS_AUTOCOMMIT.
	ServerStatusAutocommit = 0x0002
)

// A few interesting character set values.
// See http://dev.mysql.com/doc/internals/en/character-set.html#packet-Protocol::CharacterSet
const (
	// CharacterSetUtf8 is for UTF8. We use this by default.
	CharacterSetUtf8 = 33

	// CharacterSetBinary is for binary. Use by integer fields for instance.
	CharacterSetBinary = 63
)

// CharacterSetMap maps the charset name (used in ConnParams) to the
// integer value.  Interesting ones have their own constant above.
var CharacterSetMap = map[string]uint8{
	"big5":     1,
	"dec8":     3,
	"cp850":    4,
	"hp8":      6,
	"koi8r":    7,
	"latin1":   8,
	"latin2":   9,
	"swe7":     10,
	"ascii":    11,
	"ujis":     12,
	"sjis":     13,
	"hebrew":   16,
	"tis620":   18,
	"euckr":    19,
	"koi8u":    22,
	"gb2312":   24,
	"greek":    25,
	"cp1250":   26,
	"gbk":      28,
	"latin5":   30,
	"armscii8": 32,
	"utf8":     CharacterSetUtf8,
	"ucs2":     35,
	"cp866":    36,
	"keybcs2":  37,
	"macce":    38,
	"macroman": 39,
	"cp852":    40,
	"latin7":   41,
	"utf8mb4":  45,
	"cp1251":   51,
	"utf16":    54,
	"utf16le":  56,
	"cp1256":   57,
	"cp1257":   59,
	"utf32":    60,
	"binary":   CharacterSetBinary,
	"geostd8":  92,
	"cp932":    95,
	"eucjpms":  97,
}
