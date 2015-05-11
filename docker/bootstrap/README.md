# Bootstrap Images

These Dockerfiles create images that contain everything Vitess expects to have
after successfully running `bootstrap.sh` and `dev.env`.

The `vitess/bootstrap` image comes in different flavors:

* `vitess/bootstrap:common` - dependencies that are common to all flavors
* `vitess/bootstrap:mariadb` - bootstrap image for MariaDB

**NOTE: Unlike the base image that builds Vitess itself, this bootstrap image
will NOT be rebuilt automatically on every push to the Vitess master branch.**

To build a new bootstrap image, use the `docker_bootstrap` make rule.
For example:

```
~/src/github.com/youtube/vitess$ make docker_bootstrap flavor=mariadb
```

