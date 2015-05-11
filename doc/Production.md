# Production setup
Setting up vitess in production will depend on many factors.
Here are some initial considerations:

* *Global Transaction IDs*: Vitess requires a version of MySQL
that supports GTIDs.
We currently support MariaDB 10.0 and MySQL 5.6.
* *Firewalls*: Vitess tools and servers assume that they
can open direct TCP connection to each other. If you have
firewalls between your servers, you may have to add exceptions
to allow these communications.
* *Authentication*: If you need authentication, you
need to setup SASL, which is supported by Vitess.
* *Encryption:* Vitess RPC servers support SSL.
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

## Deploying in Kubernetes

See the [Getting Started](http://vitess.io/getting-started/) guide.

## Deploying on bare metal

See the
[Local Setup](https://github.com/youtube/vitess/tree/master/examples/local)
scripts for examples of how to bring up a Vitess cluster manually.
