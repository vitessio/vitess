# Tools and servers
The vitess tools and servers are designed to help you even
if you start small, and scale all the way to a complete fleet
of databases.

In the early stages, connection pooling, rowcache and other
efficiency features of vttablet help you get more from your
existing hardware.
As things scale out, the automation tools start to become handy.

### vtctl
vtctl is the main tool for performing administrative operations.
It can be used to track shards, replication graphs and
db categories.
It's also used to initiate failovers, reshard, etc.

As vtctl performs operations, it updates the necessary
changes to the lockserver (zookeeper).
The rest of the vitess servers observe those changes
and react accordingly.
For example, if a master database if failed over to a new
one, the vitess servers will see the change and redirect
future writes to the new master.

### vttablet
One of vttablet's main function is to be a proxy to MySQL.
It performs tasks that attempt to maximize throughput as
well as to protect MySQL from harmful queries. There is
one vttablet per MySQL instance.

vttablet is also capable of executing necessary management
tasks initiated from vtctl.
It also provides streaming services that are used for
filtered replication and data export.

#### vtocc
Vtocc is the previous version of vttablet. It handles query management
(same as vttablet) but is not part of a larger system, it's a standalone
program that doesn't require a Topology Server. It is useful for
unit tests and when the only required feature is the query service
(with connection pooling, query de-dup, ...).

Note we may eventually produce a version of vttablet that runs
without a Topology Server, and use it instead of vttablet.

### vtgate
vtgate's goal is to provide a unified view of the entire fleet.
It will be the server that applications will connect to for
queries. It will analyze, rewrite and route queries to various
vttablets, and return the consolidated results back to the client.

### vtctld
vtctld is an HTTP server that lets you browse the information stored
in the lockserver.
This is useful for trouble-shooting, or to get a good high
level picture of all the servers and their current state.

### vtworker
vtworker is meant to host long-running processes. It supports a plugin infrastructure, and offers libraries to easily pick tablets to use. We have developped:
- resharding differ jobs: meant to check data integrity during shard splits and joins.
- vertical split differ jobs: meant to check data integrity during vertical splits and joins.

It is very easy to add other checker processes for in-tablet integrity checks (verifying foreign key-like relationships), and cross shard data integrity (for instance, if a keyspace contains an index table referencing data in another keyspace).

### Other support tools
* *mysqlctl*: manage MySQL instances.
* *zkctl*: manage ZooKeeper instances.
* *zk*: command line ZooKeeper client and explorer.

## Vitess components block diagram
![Components](https://raw.github.com/youtube/vitess/master/doc/VitessComponents.png)
