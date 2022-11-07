# Vitess Mixin QuickStart

## Table of Contents

- [Vitess Mixin QuickStart](#vitess-mixin-quickstart)
  - [Table of Contents](#table-of-contents)
  - [What is the Vitess Mixin](#what-is-the-vitess-mixin)
  - [Development](#development)
    - [Building](#building)
      - [1. Prometheus Recording Rules](#1-prometheus-recording-rules)
      - [2. Grafana Dashboards](#2-grafana-dashboards)
    - [Formatting](#formatting)
    - [Linting](#linting)
    - [Cleaning up](#cleaning-up)
  - [Define a new Dashboard](#define-a-new-dashboard)
  - [Update Existing Dashboards](#update-existing-dashboards)
    - [Edit existing resources](#edit-existing-resources)
    - [Adding new resources](#adding-new-resources)
  - [Test and Deploy](#test-and-deploy)
    - [Local Unit Tests with `make all`](#local-unit-tests-with-make-all)
    - [Local e2e tests using Cypress (Alpha)](#local-e2e-tests-using-cypress-alpha)
    - [Local e2e manual testing using compose (Alpha)](#local-e2e-manual-testing-using-compose-alpha)

## What is the Vitess Mixin

Vitess [mixin](https://en.wikipedia.org/wiki/Mixin) is a monitoring mixin defined using [jsonnet](https://jsonnet.org/) data templating language. The grafana `jsonnet` library for creating dashboards is [grafonnet](https://github.com/grafana/grafonnet-lib). While the package manager for easily reusing and extending the mixin is [jsonnet-bundler](https://github.com/jsonnet-bundler/jsonnet-bundler).

## Development

If you want to see all available targets run `make help`.

Choose an IDE of your preference. We use VSCode with `heptio.jsonnet` plugin which includes some nice features like on mouse over documentation and (ctrl+space) autocomplete.

### Building

#### 1. Prometheus Recording Rules

  ```shell
  $ make prometheus_rules.yaml
  # Building 'prometheus_rules.yaml'...

  Done!
  ```

  Once generated the Vitess Mixin Prometheus recording rules are available at `prometheus_rules.yaml`.

  **Note:** The Vitess Mixin only includes recording rules for Prometheus Vitess metrics. Other metric like Prometheus Node are not currently included in the rules.

#### 2. Grafana Dashboards

The vitess mixin supports DEV/PROD configurations (Currently it is possible to enable/disable dashboards. And set different alert configurations for DEV/PROD).

  **Prod dashboards**:

  ```shell
  $ ENV='prod' make dashboards_out
  # Building Grafana dashboards...

  dashboards_out/cluster_overview.json
  dashboards_out/vtgate_host_view.json
  dashboards_out/vtgate_overview.json
  dashboards_out/vttablet_host_view.json

  Done!
  ```

  **Dev dashboards**:

  ```shell
  $ ENV=dev make dashboards_out

  # Building Grafana dashboards...

  dashboards_out/cluster_overview.json
  dashboards_out/vtgate_host_view.json
  dashboards_out/vtgate_overview.json
  dashboards_out/vttablet_host_view.json

  Done!
  ```

  Voila!! the generated dashboards definitions in `.json` format are now available under `/dashboards_out`. (**Note**: dev and prod dashboards have the same names and they are stored in the same output folder. It is a good practice to run `make clean` after each build to make sure we are working with a clean `/dashboards_out` folder).

### Formatting

  ```shell
  $ make fmt
  # Formatting all .libsonnet and .jsonnet files...

  Formatting dashboards/alerts_memory_events.libsonnet: ✅

  Done!
  ```

### Linting

  ```shell
  $ make lint
  # Linting all .libsonnet and .jsonnet files...

  Checking dashboards/alerts_memory_events.libsonnet: ✅

  STATUS:✅

  Done!

  # Linting 'prometheus_rules.yaml' and 'prometheus_alerts.yaml'...
  TO BE IMPLEMENTED
  Done!
  ```

### Cleaning up

  ```shell
  $ make clean
  # Cleaning up all generated files...

  rm -rf dashboards_out prometheus_alerts.yaml prometheus_rules.yaml

  Done!
  ```

## Define a new Dashboard

  Dashboards are defined in `.libsonnet` format under the `/dashboards` folder.
  **Note** Files designed for import by convention end with `.libsonnet`.

  1. Update the Mixin config to include the new **dashboard metadata** `config.libsonnet`.
      The metadata needs to be added under `grafanaDashboardMetadata`. For example for `vtgate-overview dashboard` it looks like this:

      ```bash
      vtgateOverview: {
      environments: ['dev', 'prod'],
      uid: 'vitess-vtgate-overview',
      title: 'vtgate - overview %(dashboardNameSuffix)s' % $._config.grafanaDashboardMetadataDefault,
      description: 'General vtgate overview',
      dashboardTags: $._config.grafanaDashboardMetadataDefault.dashboardTags + ['overview', 'vtgate'],
      }
      ```

      **Note**: It is possible to add/remove a dashboard from specific environments using the `environments` array field.

  2. Define the **dashboard layout**: Create a new `.libsonnet` file following _Naming Convention_ (**TODO** Define a naming convention) in the `/dashboards/layouts` folder.
    The dashboard skeleton should look something like this:

    ```js
      local helpers = import '../resources/helpers.libsonnet';

      local config = import '../../config.libsonnet';

      {
        grafanaDashboards+:: {
          'DASHBOARD_NAME.json':

          helpers.dashboard.getDashboard(config._config.grafanaDashboardMetadata.DASHBOARD_NAME)
          .addTemplates(
            [

            ]
          ).addLink(helpers.default.getDashboardLink(config._config.dashborardLinks))
          .addPanels(
            [

            ],
          ),
        },
      }
    ```

  3. Import the new dashboard in the `dashboard.libsonnet` library.

  4. Add/Edit dashboard resources.

      **Note:** Some of the resources have been grouped in the following categories `vtgate`, `vttablet`, `vtorc`, `webapp`, `os` for convenience. The resources under this categories follow a helper/config pattern. The `.*_helper.libsonnet` libraries within `grafonnet` folder functions to retrieve resources such as `getPanel(panel_name_config)`, reading the configuration by name for the desired resource from the corresponding `.*_config.libsonnet` library .

      **Note:** To isolate **PROD** and **DEV** the configuration files have a section for each environment where we can define specific `notifications`(ex: pagerduty, vitess-alerts...) and `conditions` (alert thresholds).

      In this example the alert has a different notification config. For **PROD** it pages and **DEV** it doesn't:

      ```js
        alert: {
          name: 'vitess-mixin - # of tablets with MySQL replication lag > 300s',
          frequency: '1m',
          forDuration: '3m',
          noDataState: 'ok',
          executionErrorState: 'keep_state',
          message: |||
            The number of tablets with MySQL replication lag > 300s is above the threshold.
            This usually indicate that replicas are not able to keep up with the write loads on the primary. Please take a look.
          |||,

          prod+: {
            notifications: [
              { uid: 'alerts-vitess' },
              { uid: 'pagerduty-vitess' },
            ],
            conditions: [
              alert_condition.new(
                evaluatorParams=[1],
                evaluatorType='gt',
                operatorType='and',
                queryRefId='A',
                queryTimeStart='1m',
                reducerType='max'),
            ],
          },
          //override PROD notifications.
          dev+: self.prod {
            notifications: [
              { uid: 'alerts-vitess' },
            ],
          },
        },
      ```

      **Warning:** Editing existing resources can change other dashboards behavior. Since basic resources like `panels`, `rows`, `texts`... can be shared across dashboards. This can be easily checked running `make all` as explained bellow.

## Update Existing Dashboards

  Same steps as **Define a new Dashboard** starting from step `.4`.

### Edit existing resources

  1. Look for the dashboard you want to edit in the `dashboards/layouts` folder.
  2. Find the resource you want to update in the `dashboards/resources` folder.

### Adding new resources

  1. Create the resource in the corresponding location under `dashboards/resources` folder.
  2. Add the resource to the desired layout in `dashboards/layouts`.

## Test and Deploy

### Local Unit Tests with `make all`

These tests attempt to assert truth to the following:

- Does this mixin generate valid dashboard JSON?
- Does this mixin generate valid recording rules YAML?
- Do the configured elements do what they are expected to do?

This make target will format, lint, build all generated dashboards and recording rules using `origin/main` and diff with them your `local branch`. Printing a report to the `stdout`.

```shell
  $ pwd
  /manfontan/vitess-mixin
  $ ENV='prod' make all
  #### Building origin/main

  Done!

  #### Diff origin/main with main:

  # Checking prometheus_rules.yaml... OK
  # Checking cluster_overview.json... OK
  # Checking vtgate_host_view.json... OK
  # Checking vtgate_overview.json... OK
  # Checking vttablet_host_view.json... OK

  EXIT STATUS:
  ✅ Your dashboards local version matches the origin/main version
```

The above execution shows the report for a `local branch` matching `origin/main`. Any changes will be reported as **NOK**  along with the diff report. This doesn't mean something is wrong it just points that there are changes in your local branch compared to `origin/main` which is expected. Review the diff report and once you are happy with your changes create a PR.

### Local e2e tests using Cypress (Alpha)

These tests attempt to assert truth to the following:

- Are dashboard elements displayed as expected?

The spec for each dashboard can be found at `/e2e/cypress/integration`.

`docker-compose` is used to run Cypress and Grafana. There are two targets in
[Makefile](../Makefile) to help run it.

- `make e2e`: runs tests headless and exits.

- `make e2e-dev`: opens the [the test
runner](https://docs.cypress.io/guides/core-concepts/test-runner.html#Overview)
and exposes Grafana to the host machine - http://localhost:3030. This requires
an X11 server to work. [This
post](https://www.cypress.io/blog/2019/05/02/run-cypress-with-a-single-docker-command/#Interactive-mode)
describes how to set this up with [XQuartz](https://www.xquartz.org/).

**Note** The dummy Grafana server is not connected to a Prometheus Backend for this reason dashboards will not display any data, templates will fail to load etc... If you don't have a Dev Prometheus server. Replacing prometheus datasources(`Prometheus_Vitess`, `Prometheus_Node` ...) with an empty string in the generated JSON file will default to the dummy datasource displaying dummy data. This is useful when testing using interactive mode `make e2e-dev`.

### Local e2e manual testing using compose (Alpha)

**Note**: This targets have been tested using docker for Mac. You may need to change the IPs and configurations for your specific setup.

Before we run the local environment using compose it is necessary to generate the dashboards and recording rules using `ENV=dev make all` (Note: choose the environment you want to test).

Once our dashboards are available simply run:

- `make e2e-compose-up`: spin up the cluster

Changes to the dashboards are not dynamically loaded so you will need to bring down the cluster and initialized it again to load your changes.

If you are done testing or the cluster gets in a bad state quit and clean up using:

- `make e2e-compose-down`: cleanup compose resources

In order to generate some metrics we can use the following commands:

```shell
## INSERT TEST DATA
mysql --port=15306 --host=127.0.0.1 < load_test.sql
## SIMULATED QUERIES
mysqlslap -c 5 --port=15306 --host=127.0.0.1 --iterations=1000 --create-schema=test_keyspace:80-@primary --query="SELECT * FROM messages;"
mysqlslap -c 5 --port=15306 --host=127.0.0.1 --iterations=1000 --create-schema=test_keyspace:80-@replica --query="SELECT * FROM messages;"
mysqlslap -c 5 --port=15306 --host=127.0.0.1 --iterations=1000 --create-schema=lookup_keyspace:-@primary --query="SELECT * FROM messages_message_lookup;"
mysqlslap -c 5 --port=15306 --host=127.0.0.1 --iterations=1000 --create-schema=lookup_keyspace:-@replica --query="SELECT * FROM messages_message_lookup;"
## SIMULATED ERRORS
mysqlslap --port=15306 --host=127.0.0.1 --iterations=10000 --create-schema=test_keyspace:80-@primary --query="SELECT name FROM messages;"
mysqlslap --port=15306 --host=127.0.0.1 --iterations=10000 --create-schema=lookup_keyspace:-@replica --query="SELECT name FROM messages_message_lookup;"
```

Once the cluster is up and running you should be able to access:

- grafana (default credentials > admin/admin) http://localhost:3030/
- prometheus http://localhost:9000/
- vitess control panel http://localhost:15000/
