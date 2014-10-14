# Vitess on Kubernetes

This directory contains an example configuration for running Vitess on
[Kubernetes](https://github.com/GoogleCloudPlatform/kubernetes/). Refer to the
appropriate [Getting Started Guide](https://github.com/GoogleCloudPlatform/kubernetes/#contents)
to get Kubernetes up and running if you haven't already. We currently test
against HEAD, so you may want to build Kubernetes from the latest source.

## Starting ZooKeeper

Once you have a running Kubernetes deployment, make sure
*kubernetes/cluster/kubecfg.sh* is in your path, and then run:

```
vitess$ examples/kubernetes/zk-up.sh
```

This will create a quorum of ZooKeeper servers. Clients can connect to port 2181
of any [minion](https://github.com/GoogleCloudPlatform/kubernetes/blob/master/DESIGN.md#cluster-architecture)
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
vitess/examples/kubernetes$ kubecfg.sh -c vtctld-pod.yaml create pods
vitess/examples/kubernetes$ kubecfg.sh -c vtctld-service.yaml create services
```

To access vtctld from your workstation, open up port 15000 to any minion in your
firewall. Then get the external address of that minion and visit http://<minion-addr>:15000/.

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

```
$ alias vtctl="sudo docker run -ti --rm vitess/root vtctlclient -server <minion-addr>:15000"
$ vtctl CreateKeyspace test_keyspace
$ vtctl CreateShard test_keyspace/0
```
