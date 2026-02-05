We created the `lite` image as a stripped down version of our old image `base` such that Kubernetes pods can start faster.
The `lite` image is updated automatically after every push to the GitHub main branch.

For more information on the different images we provide, please read the [`docker/README.md`](https://github.com/vitessio/vitess/tree/main/docker) file.

If your goal is run the latest Vitess code, the simplest solution is to use the `lite`.

Another alternative is to customize our Docker images and build them yourselves.
This is described below and involves building the `base` image first.
Then you can run our build script for the `lite` image which extracts the Vitess binaries from the built `base` image.

1.  Install [Docker](https://docs.docker.com/v17.12/install/) on your workstation.

    Our scripts also assume you can run the `docker` command without `sudo`,
    which you can do by [setting up a docker group](https://docs.docker.com/engine/installation/linux/ubuntulinux/#create-a-docker-group).

1.  Create an account on [Docker Hub](https://docs.docker.com/docker-hub/) and
    then `docker login` to it.

1.  Go to your `src/vitess.io/vitess` directory.

1.  Usually, you won't need to [build your own bootstrap image](https://github.com/vitessio/vitess/blob/main/docker/bootstrap/README.md)
    unless you edit [bootstrap.sh](https://github.com/vitessio/vitess/blob/main/bootstrap.sh)
    or [vendor.json](https://github.com/vitessio/vitess/blob/main/vendor/vendor.json),
    for example to add new dependencies. If you do need it then build the
    bootstrap image, otherwise pull the image using one of the following
    command.

    ```sh
    vitess$ docker pull vitess/bootstrap:latest
    ```

    **Note:** If you have already downloaded the `vitess/bootstrap:<flavor>`
    image on your machine before then it could be old, which may cause build
    failures. So it would be a good idea to always execute this step.

1.  Build the `vitess/lite[:<flavor>]` image.
    This will run a script that extracts from `vitess/bootstrap` only the files
    needed to run Vitess.

    ```sh
    vitess$ make docker_lite
    ```

1.  Re-tag the image under your personal repository, then upload it.

    ```sh
    vitess$ docker tag -f vitess/lite yourname/vitess
    vitess$ docker push yourname/vitess
    ```

    **Note:** If you chose a non-default flavor above, then change `vitess/lite` in
    the above command to `vitess/lite:<flavor>`.


1.  Launch [Vitess on Kubernetes](https://vitess.io/docs/get-started/index.html) as usual.
