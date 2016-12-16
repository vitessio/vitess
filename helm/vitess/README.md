# Vitess

[Vitess](http://vitess.io) is a database clustering system for horizontal
scaling of MySQL. It is an open-source project started at YouTube,
and has been used there since 2011.

## Introduction

This chart creates a Vitess cluster on Kubernetes in a single
[release](https://github.com/kubernetes/helm/blob/master/docs/glossary.md#release).
It currently includes all dependencies (e.g. etcd) and Vitess components
(vtctld, vtgate, vttablet) inline (in `templates/`) rather than as sub-charts.

**WARNING: This chart should be considered Alpha.
Upgrading a release of this chart may or may not delete all your data.**

## Installing the Chart

```console
helm/vitess$ helm install .
```

## Cleaning up

After deleting an installation of the chart, the PersistentVolumeClaims remain.
If you don't intend to use them again, you should delete them:

```shell
kubectl delete pvc -l app=vitess
```

## Configuration

See the comments in `values.yaml` for descriptions of the parameters.

