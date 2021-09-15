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

### Using a self-hosted runner to debug a flaky test
You will need access to the self-hosted runner machine to be able to connect to it via SSH.
1. From the output of the run on GitHub Actions, find the `Machine name`
2. Find that machine on the Equinix dashboard and connect to it via ssh
3. From the output of the `Print Volume Used` step find the volume used
4. From the output of the `Build Docker Image` step find the docker image built for this workflow
5. On the machine run `docker run -d -v <volume-name>:/vt/vtdataroot <image-name> /bin/bash -c "sleep 600000000000"`
6. On the terminal copy the docker id of the newly created container
7. Now execute `docker exec -it <docker-id> /bin/bash`
8. Use the `/vt/vtdataroot` directory to find the output of the run along with the debug files

