# Vitess on Kubernetes

This directory contains an example configuration for running Vitess on
[Kubernetes](https://github.com/GoogleCloudPlatform/kubernetes/).
Refer to the appropriate
[Getting Started Guide](https://github.com/GoogleCloudPlatform/kubernetes/tree/master/docs/getting-started-guides)
to get Kubernetes up and running if you haven't already.

## Requirements

This example was most recently tested with the
[binary release](https://github.com/GoogleCloudPlatform/kubernetes/releases)
of Kubernetes v0.9.1.

The easiest way to run the local commands like vtctl is just to install
[Docker](https://www.docker.com/)
on your workstation. You can also adapt the commands below to use a local
[Vitess build](https://github.com/youtube/vitess/blob/master/doc/GettingStarted.md)
by removing the docker preamble if you prefer.

## Starting an etcd cluster for Vitess

Once you have a running Kubernetes deployment, make sure
`kubernetes/cluster/kubectl.sh` is in your path, and then run:

```
vitess/examples/kubernetes$ ./etcd-up.sh
```

Note that these example scripts should be run from the directory they're in,
since they read other config files.

This will create two clusters: one for the 'global' cell, and one for the
'test' cell.
You can check the status of the pods with `kubectl.sh get pods` or by using the
[Kubernetes web interface](https://github.com/GoogleCloudPlatform/kubernetes/blob/master/docs/ux.md).
Note that it may take a while for each minion to download the Docker images the
first time it needs them, during which time the pod status will be `Pending`.

In general, each `-up.sh` script in this example has a corresponding `-down.sh`
in case you want to stop certain pieces without bringing down the whole cluster.
For example, to tear down the etcd deployment
(again, with `kubectl.sh` in your path):

```
vitess/examples/kubernetes$ ./etcd-down.sh
```

## Starting vtctld

The vtctld server provides a web interface to inspect the state of the system,
and also accepts RPC commands to modify the system.

```
vitess/examples/kubernetes$ ./vtctld-up.sh
```

To let you access vtctld from outside Kubernetes, the vtctld service is created
with the createExternalLoadBalancer option. On supported platforms, Kubernetes
will then automatically create an external IP that load balances onto the pods
comprising the service. Note that you also need to open port 15000 in your
firewall.

```
# open port 15000
$ gcloud compute firewall-rules create vtctld --allow tcp:15000

# get the address of the load balancer for vtctld
$ gcloud compute forwarding-rules list
NAME   REGION      IP_ADDRESS    IP_PROTOCOL TARGET
vtctld us-central1 12.34.56.78   TCP         us-central1/targetPools/vtctld

# now access vtctld at http://12.34.56.78:15000/
```

## Issuing commands with vtctlclient

If you've opened port 15000 on your firewall, you can run `vtctlclient`
locally to issue commands. Depending on your actual vtctld IP, as well as
whether you're running via Docker, the vtctlclient command will look
different. So from here on, we will assume you've made an alias called `kvtctl`
with your particular parameters, such as:

```
$ alias kvtctl="sudo docker run -ti --rm vitess/base vtctlclient -server 12.34.56.78:15000"

# check the connection to vtctld, and list available commands
$ kvtctl

# create a global keyspace record
$ kvtctl CreateKeyspace my_keyspace
```

If you don't want to open the port on the firewall, you can SSH into one of your
minions and perform the above commands against the internal IP for the vtctld
service. For example:

```
# get service IP
$ kubectl.sh get services
NAME     LABELS        SELECTOR      IP            PORT
vtctld   name=vtctld   name=vtctld   10.0.12.151   15000

# log in to a minion
$ gcloud compute ssh kubernetes-minion-1

# run a command
kubernetes-minion-1:~$ sudo docker run -ti --rm vitess/base vtctlclient -server 10.0.12.151:15000 CreateKeyspace your_keyspace
```

Note that the CreateKeyspace commands above are just for testing that
vtctlclient works. You normally would not need to do that manually,
as vttablet will initialize the necessary entries upon startup.

## Launching vttablets

We launch vttablet in a
[pod](https://github.com/GoogleCloudPlatform/kubernetes/blob/master/docs/pods.md)
along with mysqld. The following script will instantiate `vttablet-pod-template.yaml`
for three replicas.

```
vitess/examples/kubernetes$ ./vttablet-up.sh
```

Wait for the pods to enter Running state (`kubectl.sh get pods`).
Again, this may take a while if a pod was scheduled on a minion that needs to
download the Vitess Docker image. Eventually you should see the tablets show up
in the *DB topology* summary page of vtctld (`http://12.34.56.78:15000/dbtopo`).

By bringing up tablets into a previously empty keyspace, we effectively just
created a new shard. To initialize the keyspace for the new shard, we need to
perform a keyspace rebuild:

```
$ kvtctl RebuildKeyspaceGraph test_keyspace
```

Note that most vtctlclient commands produce no output on success.

### Troubleshooting

You can log into the minion corresponding to one of the pods to check the logs.
For example, on GCE that would look like this:

```
# which minion is the vttablet-101 pod on?
$ kubectl.sh get pods | grep vttablet-101
vttablet-101   [...]   kubernetes-minion-2   [...]   Running

# ssh to the minion
$ gcloud compute ssh kubernetes-minion-2

# find the Docker containers for the tablet
kubernetes-minion-2:~$ sudo docker ps | grep vttablet-101
1de8493ecc9a    vitess/root:latest   [...]    k8s_mysql...
d8c5ed2c4d53    vitess/root:latest   [...]    k8s_vttablet...
f89f0554a8aa    vitess/root:latest   [...]    k8s_net...

# exec a shell inside the mysql or vttablet container
kubernetes-minion-2:~$ sudo docker exec -ti 1de8493ecc9a bash

# look at log files for Vitess or MySQL
root@vttablet-101:vitess# cd /vt/vtdataroot/tmp
root@vttablet-101:tmp# ls
mysqlctld.INFO
vttablet.INFO
vttablet.log
root@vttablet-101:tmp# cd /vt/vtdataroot/vt_0000000101
root@vttablet-101:vt_0000000101# cat error.log
```

### Viewing vttablet status

Each vttablet serves a set of HTML status pages on its primary port.
The vtctld interface provides links on each tablet entry, but these links are
to internal per-pod IPs that can only be accessed from within Kubernetes.
As a workaround, you can proxy over an SSH connection to a Kubernetes minion,
or launch a proxy as a Kubernetes service.

The status url for each tablet is http://tablet-ip:15002/debug/status

## Electing a master vttablet

The vttablets have all been started as replicas, but there is no master yet.
When we pick a master vttablet, Vitess will also take care of connecting the
other replicas' mysqld instances to start replicating from the master mysqld.

Since this is the first time we're starting up the shard, there is no existing
replication happening, so we use the -force flag on ReparentShard to skip the
usual validation of each tablet's replication state.

```
$ kvtctl ReparentShard -force test_keyspace/0 test-0000000100
```

Once this is done, you should see one master and two replicas in vtctld's web
interface. You can also check this on the command line with vtctlclient:

```
$ kvtctl ListAllTablets test
test-0000000100 test_keyspace 0 master 10.244.4.6:15002 10.244.4.6:3306 []
test-0000000101 test_keyspace 0 replica 10.244.1.8:15002 10.244.1.8:3306 []
test-0000000102 test_keyspace 0 replica 10.244.1.9:15002 10.244.1.9:3306 []
```

## Creating a table

The vtctl tool can manage schema across all tablets in a keyspace.
To create the table defined in `create_test_table.sql`:

```
vitess/examples/kubernetes$ kvtctl ApplySchemaKeyspace -simple -sql "$(cat create_test_table.sql)" test_keyspace
```

## Launching the vtgate pool

Clients send queries to Vitess through vtgate, which routes them to the
correct vttablet(s) behind the scenes. In Kubernetes, we define a vtgate
service that distributes connections to a pool of vtgate pods curated by a
[replication controller](https://github.com/GoogleCloudPlatform/kubernetes/blob/master/docs/replication-controller.md).

```
vitess/examples/kubernetes$ ./vtgate-up.sh
```

## Launching the sample GuestBook app

The GuestBook app in this example is ported from the
[Kubernetes GuestBook example](https://github.com/GoogleCloudPlatform/kubernetes/tree/master/examples/guestbook-go).
The server-side code has been rewritten in Python to use Vitess as the storage
engine. The client-side code (HTML/JavaScript) is essentially unchanged.

```
vitess/examples/kubernetes$ ./guestbook-up.sh

# open port 3000 in the firewall
$ gcloud compute firewall-rules create guestbook --allow tcp:3000

# find the external IP of the load balancer for the guestbook service
$ gcloud compute forwarding-rules list
NAME      REGION      IP_ADDRESS     IP_PROTOCOL TARGET
guestbook us-central1 1.2.3.4        TCP         us-central1/targetPools/guestbook
vtctld    us-central1 12.34.56.78    TCP         us-central1/targetPools/vtctld
```

Once the pods are running, the GuestBook should be accessible from port 3000 on
the external IP, for example: http://1.2.3.4:3000/

Try opening multiple browser windows of the app, and adding an entry on one
side. The JavaScript on each page polls the app server once a second, so the
other windows should update automatically. Since the app serves read-only
requests by querying Vitess in 'replica' mode, this confirms that replication
is working.

See the
[GuestBook source](https://github.com/youtube/vitess/tree/master/examples/kubernetes/guestbook)
for more details.
