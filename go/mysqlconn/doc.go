// Package mysqlconn is a library to support MySQL binary protocol,
// both client and server sides.
package mysqlconn

/*

Implementation notes, collected during coding.

The reference for the capabilities is at this location:
http://dev.mysql.com/doc/internals/en/capability-flags.html

--
CLIENT_FOUND_ROWS

The doc says:
Send found rows instead of affected rows in EOF_Packet.
Value
0x00000002

Not sure what needs to be done here. Ignored for now.

--
CLIENT_CONNECT_WITH_DB:

It drives the ability to connect with a database name.
The server needs to send this flag if it supports it.
If the client supports it as well, and wants to connect with a database name,
then the client can set the flag in its response, and then put the database name
in the handshake response.

If the server doesn't support it (meaning it is not set in the server
capability flags), then the client may set the flag or not (as the
server should ignore it anyway), and then should send a COM_INIT_DB
message to set the database.

--
CLIENT_SSL:

SSL is not supported yet, in neither client nor server. It is not a lot to add.

--
PLUGABLE AUTHENTICATION:

We only support mysql_native_password for now, both client and server
side. It wouldn't be a lot of work to add SHA256 for instance.

--
Maximum Packet Size:

Set to zero by client and ignored by the server. Not sure what to do
with this now.  It seems the mysql client is sending 16777216 to the
server, which is what we use anyway. Not sure any client will send any
lower value, and if they do, not sure what the first 3 bytes of a
packet should be (still 0xff 0xff 0xff or the max packet size).

--
CLIENT_CONNECT_ATTRS

The client can send up optional connection attributes with this flags.
I don't see a use for them yet.

--
Multi result sets:

Only used by stored procedures returning multiple result sets.
Unclear if it is also used when the CLIENT_MULTI_STATEMENTS flag is used.
See: http://dev.mysql.com/doc/internals/en/multi-resultset.html

The flags returned is then used to mark if there are more result sets
coming up.

We do not support any of this yet. It would be nice to plumb that for
ExecuteBatch later on though.

--
Character sets:

See: http://dev.mysql.com/doc/internals/en/character-set.html#packet-Protocol::CharacterSet

We maintain a map of character set names to integer value.

--
Server protection:

We should add the following protections for the server:
- Limit the number of concurrently opened client connections.
- Add an idle timeout and close connections after that timeout is reached.
  Should start during initial handshake, maybe have a shorter value during
  handshake.

--
NUM_FLAG flag:

It is added by the C client library if the field is numerical.

  if (IS_NUM(client_field->type))
    client_field->flags|= NUM_FLAG;

This is somewhat useless. Also, that flag overlaps with GROUP_FLAG
(which seems to be used by the server only for temporary tables in
some cases, so it's not a big deal).

But eventually, we probably want to remove it entirely, as it is not
transmitted over the wire. For now, we keep it for backward
compatibility with the C client.

*/
