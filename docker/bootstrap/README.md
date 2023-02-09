# Bootstrap Images

These Dockerfiles create images that contain everything Vitess expects to have
after successfully running `bootstrap.sh` and `dev.env`.

The `vitess/bootstrap` image comes in different flavors:

* `vitess/bootstrap:common`    - dependencies that are common to all flavors
* `vitess/bootstrap:mysql57`   - bootstrap image for MySQL 5.7
* `vitess/bootstrap:mysql80`   - bootstrap image for MySQL 8.0
* `vitess/bootstrap:percona57` - bootstrap image for Percona Server 5.7
* `vitess/bootstrap:percona80` - bootstrap image for Percona Server 8.0

**NOTE: Unlike the base image that builds Vitess itself, this bootstrap image
will NOT be rebuilt automatically on every push to the Vitess main branch.**

To build a new bootstrap image, use the [build.sh](https://github.com/vitessio/vitess/blob/main/docker/bootstrap/build.sh)
script.

First build the `common` image, then any flavors you want. For example:

```sh
vitess$ docker/bootstrap/build.sh common
vitess$ docker/bootstrap/build.sh mysql80
```

Is it also possible to specify the resulting image name:

```sh
vitess$ docker/bootstrap/build.sh common --image my-common-image
```

If custom image names are specified, you might need to set the base image name when building flavors:

```sh
vitess$ docker/bootstrap/build.sh mysql80 --base_image my-common-image
```

Both arguments can be combined. For example:

```sh
vitess$ docker/bootstrap/build.sh mysql80 --base_image my-common-image --image my-mysql-image
```

## For Vitess Project Maintainers

To update all bootstrap images on Docker Hub, you can use the `docker_bootstrap`
Makefile target to build every flavor:

1.  Build new bootstrap images.

    ``` sh
    vitess$ make docker_bootstrap
    ```

1.  For each flavor, run all tests and make sure that they pass.

    ``` sh
    vitess$ make docker_bootstrap_test
    ```

1.  When all tests passed, push each image to Docker Hub.

    ``` sh
    vitess$ make docker_bootstrap_push
    ```

