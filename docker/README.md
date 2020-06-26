# Vitess Docker Images

The Vitess Project publishes several Docker images in the [Docker Hub "vitess" repository](https://hub.docker.com/u/vitess/).
This file describes the purpose of the different images.

**TL;DR:** Use the [vitess/lite](https://hub.docker.com/r/vitess/lite/) image for running Vitess.
Our Kubernetes Tutorial uses it as well.
Instead of using the `latest` tag, you can pin it to a known stable version e.g. `v4.0`.

## Principles

The structure of this directory and our Dockerfile files is guided by the following principles:

* The configuration of each Vitess image is in the directory `docker/<image>/`.
* Configurations for other images e.g. our internal tool Keytar (see below), can be in a different location.
* Images with more complex build steps have a `build.sh` script e.g. see [lite/build.sh](https://github.com/vitessio/vitess/blob/master/docker/lite/build.sh).
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
| **bootstrap** | manual (after incompatible changes are made to [bootstrap.sh](https://github.com/vitessio/vitess/blob/master/bootstrap.sh) or [vendor/vendor.json](https://github.com/vitessio/vitess/blob/master/vendor/vendor.json) | Basis for all Vitess images. It is a snapshot of the checked out repository after running `./bootstrap.sh`. Used to cache dependencies. Avoids lengthy recompilation of dependencies if they did not change. Our internal test runner [`test.go`](https://github.com/vitessio/vitess/blob/master/test.go) uses it to test the code against different MySQL versions. |
| **base** | automatic (after every GitHub push to the master branch) | Contains all Vitess server binaries. Snapshot after running `make build`. |
| **root** | automatic (after every GitHub push to the master branch) | Same as **base** but with the default user set to "root". Required for Kubernetes. |
| **lite** | manual (updated with every Vitess release) | Stripped down version of **base** e.g. source code and build dependencies are removed. Default image in our Kubernetes templates for minimized startup time. |

All these Vitess images include a specific MySQL/MariaDB version ("flavor").

  * We provide Dockerfile files for multiple flavors (`Dockerfile.<flavor>`).
  * On Docker Hub we publish only images with MySQL 5.7 to minimize maintenance overhead and avoid confusion.

If you are looking for a stable version of Vitess, use the **lite** image with a fixed version. If you are looking for the latest Vitess code in binary form, use the "latest" tag of the **base** image.
