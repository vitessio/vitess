## Setting up and using GitHub Self hosted runners

### Adding a new self-hosted runner
Steps to follow to add a new self-hosted runner for GitHub. 
You will need access to the Equinix account for Vitess's CI testing and Admin
access to Vitess.

1. Spawn a new c3.small instance and name it on the Equinix dashboard
2. use ssh to connect to the server
3. Install docker on the server by running the following commands
   1. `curl -fsSL https://get.docker.com -o get-docker.sh`
   2. `sudo sh get-docker.sh`
4. Create a new user with a home directory for the action runner
   1. `useradd -m github-runner`
5. Add the user to the docker group so that it can use docker as well
   1. `sudo usermod -aG docker github-runner`
6. Switch to the newly created user
   1. `su github-runner`
7. Goto the home directory of the user and follow the steps in [Adding self hosted runners to repository](https://docs.github.com/en/actions/hosting-your-own-runners/adding-self-hosted-runners#adding-a-self-hosted-runner-to-a-repository)
   1. `mkdir github-runner-<num> && cd github-runner-<num>`
   2. `curl -o actions-runner-linux-x64-2.280.3.tar.gz -L https://github.com/actions/runner/releases/download/v2.280.3/actions-runner-linux-x64-2.280.3.tar.gz`
   3. `tar xzf ./actions-runner-linux-x64-2.280.3.tar.gz`
   4. `./config.sh --url https://github.com/vitessio/vitess --token <token> --name github-runner-<num>`
   5. With a screen execute `./run.sh`
8. Set up a cron job to remove docker volumes and images every week
   1. `crontab -e`
   2. Within the file add a line `8 5 * * 6 docker system prune -f --volumes --all`
9. Vtorc, Cluster 14 and some other tests use multiple MySQL instances which are all brought up with asynchronous I/O setup in InnoDB. This sometimes leads to us hitting the Linux asynchronous I/O limit.
To fix this we increase the default limit on the self-hosted runners by -
   1. To set the aio-max-nr value, add the following line to the /etc/sysctl.conf file:
      1. `fs.aio-max-nr = 1048576`
   2. To activate the new setting, run the following command:
      1. `sysctl -p /etc/sysctl.conf`

### Moving a test to a self-hosted runner
Most of the code for running the tests is generated code by `make generate_ci_workflows` which uses the file `ci_workflow_gen.go`

To move a unit test from GitHub runners to self-hosted runners, just move the test from `unitTestDatabases` to `unitTestSelfHostedDatabases` in `ci_workflow_gen.go` and call `make generate_ci_workflows`

To move a cluster test from GitHub runners to self-hosted runners, just move the test from `clusterList` to `clusterSelfHostedList` in `ci_workflow_gen.go` and call `make generate_ci_workflows`

### Using a self-hosted runner to debug a flaky test
You will need access to the self-hosted runner machine to be able to connect to it via SSH.
1. From the output of the run on GitHub Actions, find the `Machine name` in the `Set up job` step 
2. Find that machine on the Equinix dashboard and connect to it via ssh
3. From the output of the `Print Volume Used` step find the volume used
4. From the output of the `Build Docker Image` step find the docker image built for this workflow
5. On the machine run `docker run -d -v <volume-name>:/vt/vtdataroot <image-name> /bin/bash -c "sleep 600000000000"`
6. On the terminal copy the docker id of the newly created container
7. Now execute `docker exec -it <docker-id> /bin/bash` to go into the container and use the `/vt/vtdataroot` directory to find the output of the run along with the debug files
8. Alternately, execute `docker cp <docker-id>:/vt/vtdataroot ./debugFiles/` to copy the files from the docker container to the servers local file system
9. You can browse the files there or go a step further and download them locally via `scp`.
10. Please remember to cleanup the folders created and remove the docker container via `docker stop <docker-id>`.

## Single Self-Hosted runners
There is currently one self-hosted runner which only hosts a single runner. This allows us to run tests
that do not use docker on that runner.

All that is needed to be done is to add `runs-on: single-self-hosted`, remove any code that downloads
dependencies (since they are already present on the self-hosted runner) and add a couple of lines to save
the vtdataroot output if needed.

[9944](https://github.com/vitessio/vitess/pull/9944/) is an example PR that moves one of the tests to a single-self-hosted runner.

**NOTE** - It is essential to ensure that all the binaries spawned while running the test be stopped even on failure.
Otherwise, they will keep on running until someone goes ahead and removes them manually. They might interfere
with the future runs as well.

### Using a single-self-hosted runner to debug a flaky test
The logs will be stored in the `savedRuns` directory and can be copied locally via `scp`.

A cronjob is already setup to empty the `savedRuns` directory every week so please download the runs
before they are deleted.