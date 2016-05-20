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

1.  Build the `vitess/base` image from your working copy.
    This image will incorporate any local code changes you've made,
    such as adding plugins.

    You can choose one of the following flavors:

    ```sh
    vitess$ make docker_base         # MySQL Community Edition
    vitess$ make docker_base_percona # Percona Server
    vitess$ make docker_base_mariadb # MariaDB
    ```

    **Note:** If you don't have a `vitess/bootstrap:<flavor>` image built,
    Docker will download our bootstrap image from Docker Hub.
    Usually, you won't need to [build your own bootstrap image](https://github.com/youtube/vitess/blob/master/docker/bootstrap/README.md)
    unless you edit [bootstrap.sh](https://github.com/youtube/vitess/blob/master/bootstrap.sh)
    or [vendor.json](https://github.com/youtube/vitess/blob/master/vendor/vendor.json),
    for example to add new dependencies.

1.  Build the `vitess/lite` image from the `vitess/base` image you built.
    This runs a script that extracts only the files needed to run Vitess,
    whereas `vitess/base` contains everything needed for development work.
    You will be asked to authenticate with `sudo`, which is needed to fix up
    some file permissions.

    ```sh
    vitess$ make docker_lite
    ```

1.  Re-tag the image under your personal repository, then upload it.

    ```sh
    vitess$ docker tag -f vitess/lite yourname/vitess
    vitess$ docker push yourname/vitess
    ```

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

