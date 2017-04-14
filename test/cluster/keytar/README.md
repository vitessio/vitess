# Keytar

Keytar is an internally used Vitess system for continuous execution of cluster tests on Kubernetes/Google Cloud. It monitors docker images on [Docker Hub](https://hub.docker.com). When a new image is uploaded to Docker Hub, Keytar starts a cluster on Google Compute Engine (GKE) and runs Kubernetes applications for the purpose of executing cluster tests. It will then locally run tests against the cluster. It exposes a simple web status page showing test results.

## Setup

How to set up Keytar for Vitess:

* Create service account keys with GKE credentials on the account to run the tests on. Follow [step 1 from the GKE developers page](https://developers.google.com/identity/protocols/application-default-credentials?hl=en_US#howtheywork).
* Move the generated keyfile to `$VTTOP/test/cluster/keytar/config`.
* Create or modify the test configuration file (`$VTTOP/test/cluster/keytar/config/vitess_config.yaml`).
* Ensure the configuration has the correct values for GKE project name and keyfile:
  ```
  cluster_setup:
  - type: gke
    project_name: <your_gke_project_name>
    keyfile: /config/<your_keyfile_name>
  ```
* Then run the following commands:
  ```
  > cd $VTTOP/test/cluster/keytar
  > KEYTAR_PASSWORD=<desired password> KEYTAR_PORT=<desired port, default 8080> KEYTAR_CONFIG=<desired configuration, default vitess_config.yaml> ./keytar-up.sh
  ```
* Add a Docker Hub webhook pointing to the Keytar service. The webhook URL should be in the form:
  ```
  http://<keytar-service-IP>:80/test_request?password=<KEYTAR_PASSWORD>
  ```

## Dashboard

The script to start Keytar should output a web address to view the current status. If not, the following command can also be run:
```shell
> kubectl get service keytar -o template --template '{{if ge (len .status.loadBalancer) 1}}{{index (index .status.loadBalancer.ingress 0) "ip"}}{{end}}'
```

## Limitations

Currently, Keytar has the following limitations:

* Only one configuration file allowed at a time.
* Configuration cannot be updated dynamically.
* Test results are saved in memory and are not durable.
* Results are only shown on the dashboard, there is no notification mechanism.
