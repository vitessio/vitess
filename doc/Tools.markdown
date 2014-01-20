# Tools and servers
The vitess tools and servers are designed to help you even
if you start small, and scale all the way to a complete fleet
of databases.

In the early stages, connection pooling, rowcache and other
efficiency features of vttablet help you get more from your
existing hardware.
As things scale out, the automation tools start to become handy.

![Journey](https://raw.github.com/youtube/vitess/master/doc/VitessJourney.png)

### vtctl
vtctl is the main vitess tool that for initiatiing most
administrative operations.
It can be used to track shards, replication graphs and
db categories.
It's also used to initiate failovers, resharding, etc.

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

### Other support tools
Vitess also has other support tools for diagnostics and repair.
