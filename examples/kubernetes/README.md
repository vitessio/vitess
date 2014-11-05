# Vitess on Kubernetes

This directory contains an example configuration for running Vitess on
[Kubernetes](https://github.com/GoogleCloudPlatform/kubernetes/). Refer to the
appropriate [Getting Started Guide](https://github.com/GoogleCloudPlatform/kubernetes/#contents)
to get Kubernetes up and running if you haven't already.

## Requirements

This example currently requires Kubernetes 0.4.x.
Later versions have introduced
[incompatible changes](https://groups.google.com/forum/#!topic/kubernetes-announce/idiwm36dN-g)
that break ZooKeeper support. The Kubernetes team plans to support
[ZooKeeper's use case](https://github.com/GoogleCloudPlatform/kubernetes/issues/1802)
again in the future. Until then, please *git checkout* the
[v0.4.3](https://github.com/GoogleCloudPlatform/kubernetes/tree/v0.4.3)
tag (or any newer v0.4.x) in your Kubernetes repository.

## Starting ZooKeeper

Once you have a running Kubernetes deployment, make sure
*kubernetes/cluster/kubecfg.sh* is in your path, and then run:

```
vitess$ examples/kubernetes/zk-up.sh
```

This will create a quorum of ZooKeeper servers. You can check the status of the
pods with *kubecfg.sh list pods*, or by using the
[Kubernetes web interface](https://github.com/GoogleCloudPlatform/kubernetes/blob/master/docs/ux.md).
Note that it may take a while for each minion to download the Docker images the
first time it needs them, during which time the pod status will be *Waiting*.

Clients can connect to port 2181 of any
[minion](https://github.com/GoogleCloudPlatform/kubernetes/blob/master/DESIGN.md#cluster-architecture)
(assuming the firewall is set to allow it), and the Kubernetes proxy will
load-balance the connection to any of the servers.

A simple way to test out your ZooKeeper deployment is by logging into one of
your minions and running the *zk* client utility inside Docker. For example, if
you are running [Kubernetes on Google Compute Engine](https://github.com/GoogleCloudPlatform/kubernetes/blob/master/docs/getting-started-guides/gce.md):

```
# log in to a minion
$ gcloud compute ssh kubernetes-minion-1

# show zk command usage
kubernetes-minion-1:~$ sudo docker run -ti --rm vitess/root zk

# create a test node in ZooKeeper
kubernetes-minion-1:~$ sudo docker run -ti --rm vitess/root zk -zk.addrs $HOSTNAME:2181 touch -p /zk/test_cell/vt

# check that the node is there
kubernetes-minion-1:~$ sudo docker run -ti --rm vitess/root zk -zk.addrs $HOSTNAME:2181 ls /zk/test_cell
```

To tear down the ZooKeeper deployment (again, with *kubecfg.sh* in your path):

```
vitess$ examples/kubernetes/zk-down.sh
```

## Starting vtctld

The vtctld server provides a web interface to inspect the state of the system,
and also accepts RPC commands to modify the system.

```
vitess/examples/kubernetes$ kubecfg.sh -c vtctld-service.yaml create services
vitess/examples/kubernetes$ kubecfg.sh -c vtctld-pod.yaml create pods
```

To access vtctld from your workstation, open up port 15000 to any minion in your
firewall. Then get the external address of that minion and visit *http://&lt;minion-addr&gt;:15000/*.

## Issuing commands with vtctlclient

If you've opened port 15000 on your minion's firewall, you can run *vtctlclient*
locally to issue commands:

```
# check the connection to vtctld, and list available commands
$ sudo docker run -ti --rm vitess/root vtctlclient -server <minion-addr>:15000

# create a global keyspace record
$ sudo docker run -ti --rm vitess/root vtctlclient -server <minion-addr>:15000 CreateKeyspace my_keyspace
```

If you don't want to open the port on the firewall, you can SSH into one of your
minions and perform the above commands against the minion's local Kubernetes proxy.
For example:

```
# log in to a minion
$ gcloud compute ssh kubernetes-minion-1

# run a command
kubernetes-minion-1:~$ sudo docker run -ti --rm vitess/root vtctlclient -server $HOSTNAME:15000 CreateKeyspace your_keyspace
```

## Creating a keyspace and shard

This creates the initial paths in the topology server.

```
$ alias vtctl="sudo docker run -ti --rm vitess/root vtctlclient -server <minion-addr>:15000"
$ vtctl CreateKeyspace test_keyspace
$ vtctl CreateShard test_keyspace/0
```

## Launching vttablets

We launch vttablet in a
[pod](https://github.com/GoogleCloudPlatform/kubernetes/blob/master/docs/pods.md)
along with mysqld. The following script will instantiate *vttablet-pod-template.yaml*
for a master and two replicas.

```
vitess/examples/kubernetes$ ./vttablet-up.sh
```

Wait for the pods to enter Running state (*kubecfg.sh list pods*).
Again, this may take a while if a pod was scheduled on a minion that needs to
download the Vitess Docker image. Eventually you should see the tablets show up
in the *DB topology* summary page of vtctld (*http://&lt;minion-addr&gt;:15000/dbtopo*).

### Troubleshooting

You can log into the minion corresponding to one of the pods to check the logs.
For example, on GCE that would look like this:

```
# which minion is the vttabetl-101 pod on?
$ kubecfg.sh list pods | grep vttablet-101
vttablet-101    vitess/root,vitess/root    kubernetes-minion-2    Running

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
The vtctld interface provides links on each tablet entry, but these currently
don't work when running within Kubernetes. Because there is no DNS server in
Kubernetes yet, we can't use the hostname of the pod to find the tablet, since
that hostname is not resolvable outside the pod itself. Also, we use internal
IP addresses to communicate within the cluster because in a typical cloud
environment, network fees are charged differently when instances communicate
on external IPs.

As a result, getting access to a tablet's status page from your workstation
outside the cluster is a bit tricky. Currently, this example assigns a unique
port to every tablet and then publishes that port to the Docker host machine.
For example, the tablet with UID 101 is assigned port 15101. You then have to
look up the external IP of the minion that is running vttablet-101
(via *kubecfg.sh list pods*), and visit
*http://&lt;minion-addr&gt;:15101/debug/status*. You'll of course need access
to these ports from your workstation to be allowed by any firewalls.

## Starting MySQL replication

The vttablets have been assigned master and replica roles by the startup
script, but MySQL itself has not been told to start replicating.
To do that, we do a forced reparent to the existing master.

```
$ vtctl RebuildShardGraph test_keyspace/0
$ vtctl ReparentShard -force test_keyspace/0 test_cell-0000000100
$ vtctl RebuildKeyspaceGraph test_keyspace
```

## Creating a table

The vtctl tool can manage schema across all tablets in a keyspace.
To create the table defined in *create_test_table.sql*:

```
vitess/examples/kubernetes$ vtctl ApplySchemaKeyspace -simple -sql "$(cat create_test_table.sql)" test_keyspace
```

## Launching the vtgate pool

Clients send queries to Vitess through vtgate, which routes them to the
correct vttablet(s) behind the scenes. In Kubernetes, we define a vtgate
service (currently using Services v1 on $SERVICE_HOST:15001) that load
balances connections to a pool of vtgate pods curated by a
[replication controller](https://github.com/GoogleCloudPlatform/kubernetes/blob/master/docs/replication-controller.md).

```
vitess/examples/kubernetes$ ./vtgate-up.sh
```

## Creating a client app

The file *client.py* contains a simple example app that connects to vtgate
and executes some queries. Assuming you have opened firewall access from
your workstation to port 15001, you can run it locally and point it at any
minion:

```
vitess/examples/kubernetes$ ./client.py --server=<minion-addr>:15001
Inserting into master...
Reading from master...
(1L, 'V is for speed')
Reading from replica...
(1L, 'V is for speed')
```
