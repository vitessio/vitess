# Vitess on Kubernetes

This directory contains an example configuration for running Vitess on
[Kubernetes](https://github.com/GoogleCloudPlatform/kubernetes/).

These instructions are written for running in
[Google Container Engine](https://cloud.google.com/container-engine/),
but they can be adapted to run on other
[platforms that Kubernetes supports](https://github.com/GoogleCloudPlatform/kubernetes/tree/master/docs/getting-started-guides).

## Prerequisites

If you're running Kubernetes manually, instead of through Container Engine,
make sure to use at least
[v0.9.2](https://github.com/GoogleCloudPlatform/kubernetes/releases).
Container Engine will use the latest available release by default.

You'll need [Go 1.3+](http://golang.org/doc/install) in order to build the
`vtctlclient` tool used to issue commands to Vitess:

### Build and install vtctlclient

```
$ go get github.com/youtube/vitess/go/cmd/vtctlclient
```

### Set the path to kubectl

If you're running in Container Engine, set the `KUBECTL` environment variable
to point to the `gcloud` command:

```
$ export KUBECTL='gcloud preview container kubectl'
```

If you're running Kubernetes manually, set the `KUBECTL` environment variable
to point to the location of `kubectl.sh`. For example:

```
$ export KUBECTL=$HOME/kubernetes/cluster/kubectl.sh
```

### Create a Container Engine cluster

Follow the steps to
[enable the Container Engine API](https://cloud.google.com/container-engine/docs/before-you-begin).

Set the [zone](https://cloud.google.com/compute/docs/zones#available) you want to use:

```
$ gcloud config set compute/zone us-central1-b
```

Then create a cluster:

```
$ gcloud preview container clusters create example --machine-type n1-standard-1 --num-nodes 3
```

## Start an etcd cluster for Vitess

Once you have a running Kubernetes deployment, make sure to set `KUBECTL`
as described above, and then run:

```
vitess/examples/kubernetes$ ./etcd-up.sh
```

This will create two clusters: one for the 'global' cell, and one for the
'test' cell.
You can check the status of the pods with `$KUBECTL get pods`.
Note that it may take a while for each minion to download the Docker images the
first time it needs them, during which time the pod status will be `Pending`.

In general, each `-up.sh` script in this example has a corresponding `-down.sh`
in case you want to stop certain pieces without bringing down the whole cluster.
For example, to tear down the etcd deployment:

```
vitess/examples/kubernetes$ ./etcd-down.sh
```

## Start vtctld

The vtctld server provides a web interface to inspect the state of the system,
and also accepts RPC commands from `vtctlclient` to modify the system.

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
```

In the example above, you would then access vtctld at
http://12.34.56.78:15000/ once the pod has entered the `Running` state.

## Control vtctld with vtctlclient

If you've opened port 15000 on your firewall, you can run `vtctlclient`
locally to issue commands. Depending on your actual vtctld IP,
the `vtctlclient` command will look different. So from here on, we'll assume
you've made an alias called `kvtctl` with your particular parameters, such as:

```
$ alias kvtctl='vtctlclient -server 12.34.56.78:15000'

# check the connection to vtctld, and list available commands
$ kvtctl
```

## Start vttablets

We launch vttablet in a
[pod](https://github.com/GoogleCloudPlatform/kubernetes/blob/master/docs/pods.md)
along with mysqld. The following script will instantiate `vttablet-pod-template.yaml`
for a single shard with three replicas.

```
vitess/examples/kubernetes$ ./vttablet-up.sh
```

For more tablets or shards, use the environment variables NUM_SHARDS and
TABLETS_PER_SHARD.  Example:

```
vitess/examples/kubernetes$ export NUM_SHARDS=2
vitess/examples/kubernetes$ export TABLETS_PER_SHARD=4
vitess/examples/kubernetes$ ./vttablet-up.sh
```

When using vttablet-down.sh, make sure to use the same environment variables so
all the pods are correctly brought down.

Wait for the pods to enter Running state (`$KUBECTL get pods`).
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

### Status pages for vttablets

Each vttablet serves a set of HTML status pages on its primary port.
The vtctld interface provides links on each tablet entry marked *[status]*,
but these links are to internal per-pod IPs that can only be accessed from
within Kubernetes. As a workaround, you can proxy over an SSH connection to
a Kubernetes minion, or launch a proxy as a Kubernetes service.

In the future, we plan to accomplish the proxying via the Kubernetes API
server, without the need for additional setup.

## Elect a master vttablet

The vttablets have all been started as replicas, but there is no master yet.
When we pick a master vttablet, Vitess will also take care of connecting the
other replicas' mysqld instances to start replicating from the master mysqld.

Since this is the first time we're starting up the shard, there is no existing
replication happening, so we use the -force flag on ReparentShard to skip the
usual validation of each tablet's replication state.

```
$ kvtctl ReparentShard -force test_keyspace/0 test-0000000100
$ kvtctl ReparentShard -force test_keyspace/1 test-0000000200
$ kvtctl ReparentShard -force test_keyspace/2 test-0000000300
$ kvtctl ReparentShard -force test_keyspace/3 test-0000000400
...
```

Repeat this pattern for all shards that exist in the keyspace.

Once this is done, you should see one master and two replicas in vtctld's web
interface. You can also check this on the command line with vtctlclient:

```
$ kvtctl ListAllTablets test
test-0000000100 test_keyspace 0 master 10.244.4.6:15002 10.244.4.6:3306 []
test-0000000101 test_keyspace 0 replica 10.244.1.8:15002 10.244.1.8:3306 []
test-0000000102 test_keyspace 0 replica 10.244.1.9:15002 10.244.1.9:3306 []
```

## Create a table

The `vtctlclient` tool can manage schema across all tablets in a keyspace.
To create the table defined in `create_test_table.sql`:

```
# run this from the example dir so it finds the create_test_table.sql file
vitess/examples/kubernetes$ kvtctl ApplySchemaKeyspace -simple -sql "$(cat create_test_table.sql)" test_keyspace
```

## Start a vtgate pool

Clients send queries to Vitess through vtgate, which routes them to the
correct vttablet(s) behind the scenes. In Kubernetes, we define a vtgate
service that distributes connections to a pool of vtgate pods curated by a
[replication controller](https://github.com/GoogleCloudPlatform/kubernetes/blob/master/docs/replication-controller.md).

```
vitess/examples/kubernetes$ ./vtgate-up.sh
```

## Start the sample GuestBook app server

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
for more details on how the app server interacts with Vitess.

## Tear down and clean up

Tear down the Container Engine cluster:

```
$ gcloud preview container clusters delete example
```

Clean up other entities created for this example:

```
$ gcloud compute forwarding-rules delete vtctld
$ gcloud compute firewall-rules delete vtctld
$ gcloud compute target-pools delete vtctld
```

## Troubleshooting

If a pod enters the `Running` state, but the server doesn't respond as expected,
try checking the pod output with the `kubectl log` command:

```
# show logs for container 'vttablet' within pod 'vttablet-100'
$ $KUBECTL log vttablet-100 vttablet

# show logs for container 'mysql' within pod 'vttablet-100'
$ $KUBECTL log vttablet-100 mysql
```

You can post the logs somewhere and send a link to the
[Vitess mailing list](https://groups.google.com/forum/#!forum/vitess)
to get more help.

## Automatically run Vitess on Container Engine

The following command will create a GCE cluster and bring up Vitess:
(Note that it does not bring up the Guestbook example)

```
vitess/examples/kubernetes$ ./cluster-up.sh
```

The above script accepts several environment variables, including:
NUM_SHARDS - shard count (default 1 for unsharded)
TABLETS_PER_SHARD - vttablet count for each shard

For example, to run a cluster with two shards, run:

```
export NUM_SHARDS=2
vitess/examples/kubernetes$ ./cluster-up.sh
```

Run the following to tear down the entire Vitess + GCE cluster:

```
vitess/examples/kubernetes$ ./cluster-down.sh
```
