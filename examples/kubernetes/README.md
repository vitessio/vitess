# Vitess on Kubernetes

This directory contains an example configuration for running Vitess on
[Kubernetes](https://github.com/GoogleCloudPlatform/kubernetes/).
Refer to the appropriate
[Getting Started Guide](https://github.com/GoogleCloudPlatform/kubernetes/tree/master/docs/getting-started-guides)
to get Kubernetes up and running if you haven't already.

## Requirements

This example currently assumes Kubernetes v0.6.x. We recommend downloading a
[binary release](https://github.com/GoogleCloudPlatform/kubernetes/releases).

The easiest way to run the local commands like vtctl is just to install
[Docker](https://www.docker.com/)
on your workstation. You can also adapt the commands below to use a local
[Vitess build](https://github.com/youtube/vitess/blob/master/doc/GettingStarted.md)
by removing the docker preamble if you prefer.

## Starting an etcd cluster for Vitess

Once you have a running Kubernetes deployment, make sure
*kubernetes/cluster/kubecfg.sh* is in your path, and then run:

```
vitess$ examples/kubernetes/etcd-up.sh
```

This will create two clusters: one for the 'global' cell, and one for the
'test' cell.
You can check the status of the pods with *kubecfg.sh list pods* or by using the
[Kubernetes web interface](https://github.com/GoogleCloudPlatform/kubernetes/blob/master/docs/ux.md).
Note that it may take a while for each minion to download the Docker images the
first time it needs them, during which time the pod status will be *Pending*.

Once your etcd clusters are running, you need to make a record in the global
cell to tell Vitess how to find the other etcd cells. In this case, we only
have one cell named 'test'. Since we don't want to serve etcd on external IPs,
you'll need to SSH into one of your minions and use internal IPs.

For example, if you are running
[Kubernetes on Google Compute Engine](https://github.com/GoogleCloudPlatform/kubernetes/blob/master/docs/getting-started-guides/gce.md):

```
# find the service IPs
$ kubecfg.sh list services
Name                Labels                  Selector                                  IP                  Port
----------          ----------              ----------                                ----------          ----------
etcd-test           cell=test,name=etcd     cell=test,name=etcd                       10.0.207.54         4001
etcd-global         cell=global,name=etcd   cell=global,name=etcd                     10.0.86.120         4001

# log in to a minion
$ gcloud compute ssh kubernetes-minion-1

# create a node in the global etcd that points to the 'test' cell
kubernetes-minion-1:~$ curl -L http://10.0.86.120:4001/v2/keys/vt/cells/test -XPUT -d value=http://10.0.207.54:4001
{"action":"set","node":{"key":"/vt/cells/test","value":"http://10.0.207.54:4001","modifiedIndex":9,"createdIndex":9}}
```

To tear down the etcd deployment (again, with *kubecfg.sh* in your path):

```
vitess$ examples/kubernetes/etcd-down.sh
```

## Starting vtctld

The vtctld server provides a web interface to inspect the state of the system,
and also accepts RPC commands to modify the system.

```
vitess$ examples/kubernetes/vtctld-up.sh
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

# now load up vtctld on http://12.34.56.78:15000/
```

## Issuing commands with vtctlclient

If you've opened port 15000 on your firewall, you can run *vtctlclient*
locally to issue commands:

```
# check the connection to vtctld, and list available commands
$ sudo docker run -ti --rm vitess/base vtctlclient -server 12.34.56.78:15000

# create a global keyspace record
$ sudo docker run -ti --rm vitess/base vtctlclient -server 12.34.56.78:15000 CreateKeyspace my_keyspace
```

If you don't want to open the port on the firewall, you can SSH into one of your
minions and perform the above commands against the internal IP for the vtctld
service. For example:

```
# get service IP
$ kubecfg.sh list services
Name                Labels                  Selector                                  IP                  Port
----------          ----------              ----------                                ----------          ----------
vtctld              name=vtctld             name=vtctld                               10.0.12.151         15000

# log in to a minion
$ gcloud compute ssh kubernetes-minion-1

# run a command
kubernetes-minion-1:~$ sudo docker run -ti --rm vitess/base vtctlclient -server 10.0.12.151:15000 CreateKeyspace your_keyspace
```

## Creating a keyspace and shard

This creates the initial paths in the topology server.

```
$ alias vtctl="sudo docker run -ti --rm vitess/base vtctlclient -server 12.34.56.78:15000"
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
in the *DB topology* summary page of vtctld (*http://12.34.56.78:15000/dbtopo*).

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
The vtctld interface provides links on each tablet entry, but these links are
to internal per-pod IPs that can only be accessed from within Kubernetes.
As a workaround, you can proxy over an SSH connection to a Kubernetes minion,
or launch a proxy as a Kubernetes service.

The status url for each tablet is http://tablet-ip:15002/debug/status

## Starting MySQL replication

The vttablets have been assigned master and replica roles by the startup
script, but MySQL itself has not been told to start replicating.
To do that, we do a forced reparent to the existing master.

```
$ vtctl RebuildShardGraph test_keyspace/0
$ vtctl ReparentShard -force test_keyspace/0 test-0000000100
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
service with an external IP that load balances connections to a pool of
vtgate pods curated by a
[replication controller](https://github.com/GoogleCloudPlatform/kubernetes/blob/master/docs/replication-controller.md).

```
vitess/examples/kubernetes$ ./vtgate-up.sh
```

## Creating a client app

The file *client.py* contains a simple example app that connects to vtgate
and executes some queries.

```
# open vtgate port
$ gcloud compute firewall-rules create vtgate --allow tcp:15001

# get external IP for vtgate service
$ gcloud compute forwarding-rules list
NAME   REGION      IP_ADDRESS      IP_PROTOCOL TARGET
vtgate us-central1 123.123.123.123 TCP         us-central1/targetPools/vtgate

# run client.py
$ sudo docker run -ti --rm vitess/base bash -c '$VTTOP/examples/kubernetes/client.py --server=123.123.123.123:15001'
Inserting into master...
Reading from master...
(1L, 'V is for speed')
Reading from replica...
(1L, 'V is for speed')
```
