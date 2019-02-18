## Platform support

We continuously test against Ubuntu 14.04 (Trusty) and Debian 8 (Jessie).
Other Linux distributions should work as well.

## Database support

Vitess supports [MySQL 5.6](https://dev.mysql.com/doc/refman/5.6/en/),
[MariaDB 10.0](https://downloads.mariadb.org/mariadb/10.0.21/), and any
newer versions like MySQL 5.7, etc. Vitess also supports Percona's
variations of these versions.

### Relational capabilities

Vitess attempts to leverage the capabilities of the underlying MySQL
instances to the fullest extent. In this respect, any query that can
be passed through to a single keyspace, shard or set of shards will
be sent to the MySQL servers as is.

This approach allows you to exploit the full capabilities of MySQL
as long as the relationships and constraints are within one shard (or
unsharded keyspace).

For relationships that go beyond shards, Vitess provides 
support through the [VSchema]({% link user-guide/vschema.md %}).

### Schema management

Vitess supports several functions for looking at your schema and
validating its consistency across tablets in a shard or across all
shards in a keyspace.

In addition, Vitess supports 
[data definition statements](https://dev.mysql.com/doc/refman/5.6/en/sql-syntax-data-definition.html)
that create, modify, or delete database tables. Vitess executes
schema changes on the master tablet within each shard, and those
changes then propagate to slave tablets via replication. Vitess does
not support other types of DDL statements, such as those that affect
stored procedures or grants.

Before executing a schema change, Vitess validates the SQL syntax
and determines the impact of the change. It also does a pre-flight
check to ensure that the update can be applied to your schema. In
addition, to avoid reducing the availability of your entire system,
Vitess rejects changes that exceed a certain scope.

See the [Schema Management]({% link user-guide/schema-management.md %})
section of this guide for more information.

## Supported clients

The VTGate server is the main entry point that applications use
to connect to Vitess.

VTGate understands the MySQL binary protocol. So, any client that
can directly talk to MySQL can also use Vitess.

Additionally, VTGate exposes its functionality through a
[gRPC](https://www.grpc.io/) API which has support for multiple languages.

Accessing Vitess through gRPC has some minor advantages over the MySQL
protocol:

* You can send requests with bind variables, which is slightly more
  efficient and secure than building the full text query.
* You can exploit the statelessness of connections. For example, you
  can start a transaction using one VTGate server, and complete it
  using another.

Vitess currently provides gRPC based connectors for Java (JDBC) and Go
(database/sql). All others can use the native MySQL drivers instead. The
native MySQL drivers for Java and Go should also work.

## Backups

Vitess supports data backups to either a network mount (e.g. NFS) or to a blob store.
Backup storage is implemented through a pluggable interface,
and we currently have plugins available for Google Cloud Storage, Amazon S3,
and Ceph.

See the [Backing Up Data]({% link user-guide/backup-and-restore.md %}) section
of this guide for more information about creating and restoring data
backups with Vitess. 
