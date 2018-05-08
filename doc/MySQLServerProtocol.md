# MySQL Binary Protocol

Vitess supports MySQL binary protocol. This allows existing applications to connect to Vitess directly without any change, or without using a new driver or connector. This is now the recommended and the most popular protocol for connecting to Vitess.

# Features of RPC protocol not supported by SQL protocol

### Bind Variables
The RPC protocol supports bind variables which allows Vitess to cache query plans providing much better execution times.

### Event Tokens
The RPC protocols allows you to use event tokens to get the latest binlog position. These can be used for cache invalidation.

### Update Stream
Update stream allows you to subscribe to changing rows.

### Query Multiplexing
Ability to multiplex multiple request/responses on the same TCP connection.