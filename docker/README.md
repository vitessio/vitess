# Vitess Docker Images

The Vitess Project publishes several Docker images in the [Docker Hub "vitess" repository](https://hub.docker.com/u/vitess/).
This file describes the purpose of the different images.

**TL;DR:** Use the [vitess/lite](https://hub.docker.com/r/vitess/lite/) image for running Vitess.
Our Kubernetes Tutorial uses it as well.
Instead of using the `latest` tag, you can pin it to a known stable version e.g. `v2.0`.

## Principles

The structure of this directory and our Dockerfile files is guided by the following principles:

* The configuration of each Vitess image is in the directory `docker/<image>/`.
* Configurations for other images e.g. our internal tool Keytar (see below), can be in a different location.
* Images with more complex build steps have a `build.sh` script e.g. see [lite/build.sh](https://github.com/youtube/vitess/blob/master/docker/lite/build.sh).
* Tags are used to provide (stable) versions e.g. see tag `v2.0` for the image [vitess/lite](https://hub.docker.com/r/vitess/lite/tags).
* Where applicable, we provide a `latest` tag to reference the latest build of an image.

## Images

Our list of images can be grouped into:

* published Vitess code
* dependencies for our Kubernetes tutorial
* internally used tools

### Vitess

| Image | How (When) Updated | Description |
| --- | --- | --- |
| **bootstrap** | manual (after incompatible changes are made to [bootstrap.sh](https://github.com/youtube/vitess/blob/master/bootstrap.sh) or [vendor/vendor.json](https://github.com/youtube/vitess/blob/master/vendor/vendor.json) | Basis for all Vitess images. It is a snapshot of the checked out repository after running `./bootstrap.sh`. Used to cache dependencies. Avoids lengthy recompilation of dependencies if they did not change. Our internal test runner [`test.go`](https://github.com/youtube/vitess/blob/master/test.go) uses it to test the code against different MySQL versions. |
| **base** | automatic (after every GitHub push to the master branch) | Contains all Vitess server binaries. Snapshot after running `make build`. |
| **root** | automatic (after every GitHub push to the master branch) | Same as **base** but with the default user set to "root". Required for Kubernetes. |
| **lite** | manual (updated with every Vitess release) | Stripped down version of **base** e.g. source code and build dependencies are removed. Default image in our Kubernetes templates for minimized startup time. |

All these Vitess images include a specific MySQL/MariaDB version ("flavor").

  * We provide Dockerfile files for multiple flavors (`Dockerfile.<flavor>`).
  * As of April 2017, the following flavors are supported: `mariadb`, `mysql56`, `mysql57`, `percona`(56), `percona57`
  * On Docker Hub we publish only images with MySQL 5.7 to minimize maintenance overhead and avoid confusion.
  * If you need an image for a different flavor, it is very easy to build it yourself. See the [Custom Docker Build instructions](http://vitess.io/getting-started/docker-build/).

If you are looking for a stable version of Vitess, use the **lite** image with a fixed version. If you are looking for the latest Vitess code in binary form, use the "latest" tag of the **base** image.

### Kubernetes Tutorial Dependencies

| Image | How (When) Updated | Description |
| --- | --- | --- |
| **etcd-lite** | manual | Our Kubernetes tutorial uses etcd as Vitess topology backing store and runs this image. It is a stripped version of the **etcd** image for faster Kubernetes startup times. Published as `vitess/etcd:<version>-lite` e.g. [v2.0.13-lite](https://hub.docker.com/r/vitess/etcd/tags/). |
| **etcd** | manual | Basis for **etcd-lite**. |
| **guestbook** | manual (updated with every Vitess release) | Vitess adaption of the Kubernetes guestbook example. Used to showcase sharding in Vitess. Dockerfile is located in [`examples/kubernetes/guestbook/`](https://github.com/youtube/vitess/tree/master/examples/kubernetes/guestbook). |
| **orchestrator** | manual | Binaries for [Orchestrator](https://github.com/github/orchestrator). It can be used with Vitess for automatic failovers. Currently not part of the Kubernetes Tutorial and only used in tests. |

### Internal Tools

These images are used by the Vitess project for internal workflows and testing infrastructure and can be ignored by users.

| Image | How (When) Updated | Description |
| --- | --- | --- |
| **publish-site** | manual | Contains [Jekyll](https://jekyllrb.com/) which we use to generate our [vitess.io](http://vitess.io) website from the Markdown files located in [doc/](https://github.com/youtube/vitess/tree/master/doc). |
| **keytar** | manual | Keytar is a Vitess testing framework to run our Kubernetes cluster tests. Dockerfile is located in [`test/cluster/keytar/`](https://github.com/youtube/vitess/tree/master/test/cluster/keytar). |
