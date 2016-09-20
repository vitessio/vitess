## Platform support

We continuously test against Ubuntu 14.04 (Trusty) and Debian 8 (Jessie).
Other Linux distributions should work as well.

## Database support

Vitess supports [MySQL 5.6](http://dev.mysql.com/doc/refman/5.6/en/),
[MySQL 5.7](http://dev.mysql.com/doc/refman/5.7/en/),
and [MariaDB 10.0](https://downloads.mariadb.org/mariadb/10.0.21/).

### Data types and SQL support

In Vitess, database tables are like MySQL relational tables, and you
can use relational modeling schemes (normalization) to structure your
schema. Vitess supports both primary and secondary indexes.

Vitess supports almost all MySQL scalar data types.
It also provides full SQL support within a
[shard](/overview/concepts.html#shard), including JOIN statements.

Vitess does not currently support encoded protobufs or protocol buffer
querying. (The latter is also known as cracking.) Protocol buffers can
be stored as a blob in MySQL, but must be decoded and interpreted at
the application layer.

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

See the [Schema Management](/user-guide/schema-management.html)
section of this guide for more information.

## Supported clients

You can access your Vitess cluster using a variety of clients and
programming languages.

Vitess' service is exposed through a
[proto3](https://developers.google.com/protocol-buffers/docs/proto3)
service definition. Vitess supports [gRPC](http://www.grpc.io/),
and you can use the 
[proto compiler](https://developers.google.com/protocol-buffers/docs/proto?hl=en#generating)
to generate stubs that can call the API in any language that the
gRPC framework supports.

### Client libraries

Client libraries that support a richer set of functionality are
available for some languages. Client libraries help your application
to more easily talk to your storage system to query data.

The following table lists those
client libraries and other clients that Vitess supports.

| Type | Options |
| :-------- | :--------- |
| Client library | [gRPC](http://www.grpc.io/)<br class="bigbreak">Go<br class="bigbreak">Java<br class="bigbreak">Python<br class="bigbreak">PHP |
| MapReduce | [Hadoop input](https://hadoop.apache.org/docs/r2.7.0/api/org/apache/hadoop/mapreduce/InputFormat.html) |

## Backups

Vitess supports data backups to either a network mount (e.g. NFS) or to a blob store.
Backup storage is implemented through a pluggable interface,
and we currently have plugins available for Google Cloud Storage, Amazon S3,
and Ceph.

See the [Backing Up Data](/user-guide/backup-and-restore.html) section
of this guide for more information about creating and restoring data
backups with Vitess. 
