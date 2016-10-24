By default, the [Kubernetes configs](https://github.com/youtube/vitess/tree/master/examples/kubernetes)
point to the `vitess/lite` image on [Docker Hub](https://hub.docker.com/u/vitess/).
This image is built periodically from the master branch on GitHub.

If you want to customize this image, you can build your own like this:

1.  Install [Docker](https://www.docker.com/) on your workstation.

    Our scripts also assume you can run the `docker` command without `sudo`,
    which you can do by [setting up a docker group](https://docs.docker.com/engine/installation/linux/ubuntulinux/#create-a-docker-group).

1.  Create an account on [Docker Hub](https://docs.docker.com/docker-hub/) and
    then `docker login` to it.

1.  Go to your `github.com/youtube/vitess` directory.

1.  Usually, you won't need to [build your own bootstrap image]
    (https://github.com/youtube/vitess/blob/master/docker/bootstrap/README.md)
    unless you edit [bootstrap.sh](https://github.com/youtube/vitess/blob/master/bootstrap.sh)
    or [vendor.json](https://github.com/youtube/vitess/blob/master/vendor/vendor.json),
    for example to add new dependencies. If you do need it then build the
    bootstrap image, otherwise pull the image using one of the following
    commands depending on the MySQL flavor you want:

    ```sh
    vitess$ docker pull vitess/bootstrap:mysql57   # MySQL Community Edition 5.7
    vitess$ docker pull vitess/bootstrap:mysql56   # MySQL Community Edition 5.6
    vitess$ docker pull vitess/bootstrap:percona57 # Percona Server 5.7
    vitess$ docker pull vitess/bootstrap:percona   # Percona Server
    vitess$ docker pull vitess/bootstrap:mariadb   # MariaDB
    ```

    **Note:** If you have already downloaded the `vitess/bootstrap:<flavor>`
    image on your machine before then it could be old, which may cause build
    failures. So it would be a good idea to always execute this step.

1.  Build the `vitess/lite[:<flavor>]` image. This will build
    `vitess/base[:<flavor>]` image and run a script that extracts only the files
    needed to run Vitess (`vitess/base` contains everything needed for
    development work). You will be asked to authenticate with `sudo`, which is
    needed to fix up some file permissions.

    Choose one of the following commands (the command without suffix builds
    default image containing MySQL 5.7):

    ```sh
    vitess$ make docker_lite
    vitess$ make docker_lite_mysql56
    vitess$ make docker_lite_percona57
    vitess$ make docker_lite_percona
    vitess$ make docker_lite_mariadb
    ```

1.  Re-tag the image under your personal repository, then upload it.

    ```sh
    vitess$ docker tag -f vitess/lite yourname/vitess
    vitess$ docker push yourname/vitess
    ```

    **Note:** If you chose non-default flavor above then change `vitess/lite` in
    the above command to `vitess/lite:<flavor>`.

1.  Change the Kubernetes configs to point to your personal repository:

    ```sh
    vitess/examples/kubernetes$ sed -i -e 's,image: vitess/lite,image: yourname/vitess:latest,' *.yaml
    ```

    Adding the `:latest` label at the end of the image name tells Kubernetes
    to check for a newer image every time a pod is launched.
    When you push a new version of your image, any new pods will use it
    automatically without you having to clear the Kubernetes image cache.

    Once you've stabilized your image, you'll probably want to replace `:latest`
    with a specific label that you change each time you make a new build,
    so you can control when pods update.

1.  Launch [Vitess on Kubernetes](http://vitess.io/getting-started/) as usual.

