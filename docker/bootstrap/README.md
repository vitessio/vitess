# Bootstrap Images

These Dockerfiles create images that contain everything Vitess expects to have
after successfully running `bootstrap.sh` and `dev.env`.

The `vitess/bootstrap` image comes in different flavors:

* `vitess/bootstrap:common` - dependencies that are common to all flavors
* `vitess/bootstrap:mariadb` - bootstrap image for MariaDB
* `vitess/bootstrap:mysql56` - bootstrap image for MySQL 5.6
* `vitess/bootstrap:percona` - bootstrap image for Percona Server

**NOTE: Unlike the base image that builds Vitess itself, this bootstrap image
will NOT be rebuilt automatically on every push to the Vitess master branch.**

To build a new bootstrap image, use the [build.sh](https://github.com/youtube/vitess/blob/master/docker/bootstrap/build.sh)
script.

First build the `common` image, then any flavors you want. For example:

```sh
vitess$ docker/bootstrap/build.sh common
vitess$ docker/bootstrap/build.sh mysql56
```

## For Vitess Project Maintainers

To update all bootstrap images on Docker Hub, you can use the `docker_bootstrap`
Makefile target to build every flavor:

1.  Build new bootstrap images.

    ``` sh
    make docker_bootstrap
    ```

1.  For each flavor, run all tests and make sure that they pass.

    ``` sh
    vitess$ ./test.go -pull=false -flavor=mariadb
    vitess$ ./test.go -pull=false -flavor=mysql56
    ...
    ```

1.  When all tests passed, push each image to Docker Hub.

    ``` sh
    $ docker push vitess/bootstrap:common
    $ docker push vitess/bootstrap:mariadb
    $ docker push vitess/bootstrap:mysql56
    ...
    ```

