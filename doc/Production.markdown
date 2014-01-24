# Production setup
Setting up vitess in production will depend on many factors.
Here are some initial considerations:
* *Do you have a firewall between servers?* Vitess tools
and servers assume that they can open direct TCP connection
to each other.
* *Do you trust your clients?* If you don't, then you'll
need to setup SASL authentication, which is supported by
Vitess.
* *Do you trust your connections?* If you're worried about
packet sniffing, you'll need to enable SSL for the RPCs,
which is also supported.
* *MySQL permissions*: Vitess currently assumes that all
application clients have uniform permissions.
The connection pooler opens a number of connections under
the same user (vt_app), and rotates them for all requests.
Vitess management tasks use a different user name (vt_dba),
which is assumed to have all administrative privileges.
* *What is your client language?* We currently support
Python and Go.
It's not too hard to add support for more languages,
and are open to contributions in this area.

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
