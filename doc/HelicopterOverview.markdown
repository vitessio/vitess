# Helicopter overview

This is a very high level overview of Vitess. It doesn’t get into the technical
details – it’s goal is to help you understand if Vitess can solve your problems,
and what kind of effort it would require for you to start using Vitess.

## When do you need Vitess?

  - You store all your data in a MySQL database, and have a significant number of
  clients. At some point, you start getting Too many connections errors from
  MySQL, so you have to change the max_connections system variable. Every MySQL
  connection has a memory overhead, which is just below 3 MB in the default
  configuration. If you want 1500 additional connections, you will need over 4 GB
  of additional RAM – and this is not going to be contributing to faster queries.

  - From time to time, your developers make mistakes. For example, they make your
  app issue a query without setting a LIMIT, which makes the database slow for
  all users. Or maybe they issue updates that break statement based replication.
  Whenever you see such a query, you react, but it usually takes some time and
  effort to get the story straight.

  - You store your data in a MySQL database, and your database has grown
  uncomfortably big. You are planning to do some horizontal sharding. MySQL
  doesn’t support sharding, so you will have write the code to perform the
  sharding, and then bake all the sharding logic into your app.

  - You run a MySQL cluster, and use replication for availability: you have a master
  database and a few replicas, and in the case of a master failure some replica
  should become the new master. You have to manage the lifecycle of the databases,
  and communicate the current state of the system to the application.

  - You run a MySQL cluster, and have custom database configurations for different
  workloads. There’s the master where all the writes go, fast read-only replicas
  for web clients, slower read-only replicas for batch jobs, and another kind of
  slower replicas for backups. If you have horizontal sharding, this setup is
  repeated for every shard. The code that your app uses to find the right
  database is rather complicated, and you have to deal with constantly updating
  the configuration.

If your MySQL installation is similar to any of the three scenarios described
above, you may benefit from taking a closer look at Vitess. YouTube suffered
from an extreme case of all of these problems – we serve a lot of traffic – and
Vitess helped us alleviate them.

## How Vitess can help?

### Connection pooling

Instead of using MySQL’s protocol, clients connect to Vitess uses its almost
stateless, BSON based protocol. These connections are very lightweight (around
32 KB per connections), which means our servers can handle thousands of them
without breaking a sweat. These connections are efficiently mapped to a pool of
MySQL connections thanks to Go’s awesome concurrency support.

### Handling queries

When Vitess gets a query, it goes through a SQL parser, which informs the
decision how to proceed with the query. If the query looks like it could have an
avoidable negative impact on the performance, it will be rewritten according to
a configurable set of rules. For example, queries without a limit will get a
default limit of 10 thousand rows. If at the time a client issues a query the
exact same query is already in flight, the database will do the work only once,
returning the same result to both clients.

### Sharding

Vitess has sharding built in, and it facilitates sharding with minimal downtime.
For example, it supports split replication, in which the replication stream can
be divided in such a way that a future shard master will get only statements
that could possibly affect rows in its new shard. This allows us to perform a
resharding with only a few minutes of read-only downtime for the shard.

What is more, if you already have your own, custom sharding scheme in place,
Vitess is fine with that: you can easily keep using it for its other features.

### Workflow management

Vitess can help you manage the lifecycle of your database instances. It supports
various scenarios, like master failover, backups, etc. – and all of them are
handled automatically, minimizing any necessary downtime.

### Limiting complexity

All the metadata about the Vitess cluster (sharding scheme, running instances,
their health, work profile, etc.) is stored in the Topology, which is backed by
a consistent data store, like ZooKeeper. This means that the app’s cluster view
is always up to date and consistent for different clients. What is more, your
app doesn’t need to know anything about all this. It connects to Vitess servers
through vtgate, a lightweight proxy which handles routing the queries to the
appropriate instances. This allows the Vitess client to be extremely simple –
it’s just an implementation of a few API calls.

## Starting easy: vtocc

If you are only interested in connection pooling and handling queries, you are
in luck: it should be simple to start using those features without making
significant changes to your infrastructure. All you need is to run vtocc in
front of your MySQL database, and change your app to use the Vitess client
instead of your MySQL driver. This is how we launched Vitess into production at
YouTube

## Going all the way: vttablet

Getting a fully functional Vitess installation is a quite involved affair, and
it requires some planning.

### Topology backend

Vitess needs to store its metadata in some place. This data storage needs to
have different properties than a regular database. It doesn’t need to support a
lot of traffic or data, but the data should be consistent and be able to survive
failures in the machines that host it (so, we are interested in Consistency and
Partition Tolerance from the [CAP
theorem](http://en.wikipedia.org/wiki/CAP_theorem)). This means that the ideal
candidates for this role implement
[Paxos](http://en.wikipedia.org/wiki/Paxos_(computer_science)) or something
equivalent.

Out of the box, Vitess supports [Apache
ZooKeeper](http://zookeeper.apache.org/), which is a well battle tested
distributed lock service. If you have a reason not to use ZooKeeper, it should
be relatively easy to write a Vitess plugin to use something like Etcd, Consul,
or Doozer. At Google, for example, we use Chubby through the plugin mechanism.

### Preparing the data

Vitess has the notion of a keyspace – a logical database that may consist of
many shards. If you handle sharding yourself or don’t need it, this is just a
matter of choosing a name (your database name is a reasonable choice). If you
want Vitess to handle sharding, or think you may need that in the feature,
there’s a bit more work that you should do.

First, you have to think about your sharding scheme. This is quite a big topic
by itself, but the general idea is that data that is used together should be
kept on the same shard. For example, if your app deals with users and objects
created by those users, the user id is a good candidate to build your sharding
schema around. The choices you make about your sharding scheme are crucial. One
of the limitations of Vitess (and any other sharding solution for MySQL) is that
transactions will not work across shard boundaries.

Once you know what you want to base your sharding key on, you have to add this
data to the database. You should create an additional column in all your sharded
tables that’s big enough to contain a 64 bit integer and populate it.

### vttablets

Now that the data is ready for Vitess, you can start launching vttablets. There
should be exactly one vttablet for every MySQL database, and you can tag
different vttablets for different kind of jobs. After you start an instance that
connects to its database, you let the system know about it by using vtctl, a
command line tool.

### Clients

Your app won’t connect to the tablets directly – it will always go through
vtgate. There are many ways to setup vtgate. You can have a pool of them, or you
can start one per process or group of processes (it doesn’t consume much
resources). All your app needs to do is to use the Vitess client and somehow let
it know how to connect to a running vtgate instance.
