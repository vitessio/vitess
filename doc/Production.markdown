# Production setup
Setting up vitess in production will depend on many factors.
Here are some initial considerations:
* *Global Transaction IDs*: Vitess requires a version of MySQL
that supports GTIDs, such as Google MySQL 5.1+, MariaDB 10.0+,
or MySQL 5.6+. We currently support Google MySQL and MariaDB,
with plans to add MySQL 5.6.
* *Firewalls*: Vitess tools and servers assume that they
can open direct TCP connection to each other. If you have
firewalls between your servers, you may have to add exceptions
to allow these communications.
* *Authentication*: If you need authentication, you
need to setup SASL, which is supported by Vitess.
* *Encryption:* Vitess RPC servers support SSL.
TODO: Document how to setup SSL.
* *MySQL permissions*: Vitess currently assumes that all
application clients have uniform permissions.
The connection pooler opens a number of connections under
the same user (vt_app), and rotates them for all requests.
Vitess management tasks use a different user name (vt_dba),
which is assumed to have all administrative privileges.
* *Client Language*: We currently support
Python and Go.
It's not too hard to add support for more languages,
and we are open to contributions in this area.

## Setting up Zookeeper
### Global zk setup
TODO: Explain
### Local zk setup
TODO: Explain

## Launch vttablets
vttablet is designed to run on the same machine as mysql.
You'll need to launch one instance of vttablet for every MySQL instance you want to track.

TODO: Specify order and command-line arguments

## Launch vtgate(s)
TODO: Explain
