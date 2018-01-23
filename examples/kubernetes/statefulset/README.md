# Vitess StatefulSet Demo

This demo shows how StatefulSet works with Vitess by doing the following:

1. Create a sharded Vitess cluster through a Helm chart.
1. Start a loadtest script to send queries to Vitess.
1. Scale a Vitess StatefulSet.
1. Simulate MySQL master failure to test automated failover.

## Prerequisites

* To run the interactive `demo.sh` script, you need to install [`pv`]
  (http://man7.org/linux/man-pages/man1/pv.1.html).
  Alternatively, you can run the commands in that script manually.

* You need to create a Google Cloud Storage bucket under the same project as
  your Kubernetes cluster.

  ```shell
  gsutil mb gs://<bucket-name>
  ```

  Fill in your bucket name in `site-values.yaml` and `demo.sh`.

* You need to give the `storage-rw` scope to your GCE instances so they can
  access the GCS bucket.

  You can do that when starting Kubernetes like this:

  ```shell
  export NODE_SCOPES=compute-rw,monitoring,logging-write,storage-rw
  cluster/kube-up.sh
  ```

* You need Kubernetes 1.6+ with [Helm](https://github.com/kubernetes/helm)
  installed into it (i.e. with `helm init`).

* Start a local proxy into the Kubernetes apiserver so you can load the
  Vitess web console (vtctld).

  ```shell
  kubectl proxy --port=8001
  ```

* You need to have `vtctlclient` installed locally (and available in PATH)
  to send commands to Vitess:

  ```shell
  export GOPATH=$HOME
  export PATH=$PATH:$GOPATH/bin
  go get github.com/youtube/vitess/go/cmd/vtctlclient
  ```

## Clean up

The interactive script runs some cleanup tasks for you at the end.
If you abort in the middle, you should run those commands manually.

You should also manually delete the GCS bucket you created:

```shell
gsutil rm -R gs://<bucket-name>
```

