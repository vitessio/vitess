// Package replication contains data structure definitions for MySQL
// replication related features.
package replication

/*

This package defines a few data structures used for replication.
It is meant to only depend on the proto definitions, and nothing else.
go/mysqlconn uses this package to handle replication-related functions.

It has two main aspects:

1. Replication event and positions.

A replication event is an individual event, and it has an ID, called GTID.

A replication position is defined slightly differently for MariaDB and MySQL 5.6+:

- MariaDB uses the latest position as an integer, that assumes every
  single event before that integer was applied. So a replication
  position is similar to a GTID.

- Mysql 5.6+ keeps track of all event ever applied, in a structure called GTIDSet.

To make these two compatible, a replication position is defined by
this library as a GTIDSet. For MariaDB, the Set is equal to a Position.


2. Binlog event management. They are defined in the MySQL spec at:
http://dev.mysql.com/doc/internals/en/replication-protocol.html

These are slightly different for MariaDB and MySQL 5.6+, as they
contain GTIDs. MariaDB also defines a GTID event that has an implicit
Begin, that can replace an usual Begin.

*/
