# Vitess Docker Images

The Vitess Project publishes several Docker images in
the [Docker Hub "vitess" repository](https://hub.docker.com/u/vitess/). This file describes the purpose of the different
images.

**TL;DR:** Use the [vitess/lite](https://hub.docker.com/r/vitess/lite/) image for running Vitess. Our Kubernetes
Tutorial uses it as well. Instead of using the `latest` tag, you can pin it to a known stable version e.g. `v4.0`.

## Principles

The structure of this directory and our Dockerfile files is guided by the following principles:

* The configuration of each Vitess image is in the directory `docker/<image>/`.
* Configurations for other images e.g. our internal tool Keytar (see below), can be in a different location.
* Images with more complex build steps have a `build.sh` script e.g.
  see [bootstrap/build.sh](https://github.com/vitessio/vitess/blob/main/docker/bootstrap/build.sh).
* Tags are used to provide (stable) versions e.g. see tag `v2.0` for the
  image [vitess/lite](https://hub.docker.com/r/vitess/lite/tags).
* Where applicable, we provide a `latest` tag to reference the latest build of an image.

## Images

Our list of images can be grouped into:

* published Vitess code
* dependencies for our Kubernetes tutorial
* internally used tools

### Vitess

| Image | How (When) Updated | Description |
| --- | --- | --- |
| bootstrap | manual (after incompatible changes are made to [bootstrap.sh](https://github.com/vitessio/vitess/blob/main/bootstrap.sh) or [vendor/vendor.json](https://github.com/vitessio/vitess/blob/main/vendor/vendor.json) | Basis for all Vitess images. It is a snapshot of the checked out repository after running `./bootstrap.sh`. Used to cache dependencies. Avoids lengthy recompilation of dependencies if they did not change. Our internal test runner [`test.go`](https://github.com/vitessio/vitess/blob/main/test.go) uses it to test the code against different MySQL versions. |
| base | manual (on demand) | Contains all Vitess server binaries. Snapshot after running `make build`. |
| **lite** | automatic (after every push to main branch) | Stripped down version of base

All these Vitess images include a specific MySQL/MariaDB version ("flavor").

* We provide Dockerfile files for multiple flavors (`Dockerfile.<flavor>`).
* On Docker Hub we publish only images with MySQL 5.7 to minimize maintenance overhead and avoid confusion.

If you are looking for a stable version of Vitess, use the **lite** image with a fixed version. If you are looking for
the latest Vitess code in binary form, use the "latest" tag of the **lite** image. 
If you need to use a binary that is not included in **lite** use the **base** image.