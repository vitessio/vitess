# Advanced Vitess on Kubernetes

## Automatically run Vitess on Container Engine

The following command will create a Google Container Engine cluster and bring
up Vitess with two shards and three tablets per shard: (Note that it does not
bring up the Guestbook example)

```
vitess/examples/kubernetes$ ./cluster-up.sh
```

Run the following to tear down the entire Vitess + container engine cluster:

```
vitess/examples/kubernetes$ ./cluster-down.sh
```

## Parameterizing configs

The vttablet and cluster scripts both support parameterization via exporting
environment variables.

### Parameterizing cluster scripts

The cluster-up.sh script supports the following environment variables:
* GKE_ZONE - Zone to use for Container Engine (default us-central1-b)
* GKE_MACHINE_TYPE - Container Engine machine type (default n1-standard-1)
* GKE_NUM_NODES - Number of nodes to use for the cluster (default 3).
* GKE_CLUSTER_NAME - Name to use when creating a cluster (default example).
* SHARDS - Comma delimited keyranges for shards (default -80,80- for 2 shards).
Use 0 for an unsharded keyspace.
* TABLETS_PER_SHARD - Number of shards per shard (default 3).

For example, to create an unsharded keyspace with 5 tablets, use the following:

```
export SHARDS=0
export TABLETS_PER_SHARD=5
vitess/examples/kubernetes$ ./cluster-up.sh
```

### Parameterizing vttablet scripts

Both SHARDS and TABLETS_PER_SHARD from cluster-up.sh apply to vttablet-up.sh.


