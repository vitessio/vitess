# (Alpha) Prometheus Monitoring Mixin for Vitess

A set of Grafana dashboards and Prometheus alerts for Vitess.

## Prerequisites

1. Install `jsonnet-bundler`:
    * via `go`: `go get github.com/jsonnet-bundler/jsonnet-bundler/cmd/jb`
    * via `brew`: `brew install jsonnet`

1. Install `promtool`: `go get github.com/prometheus/prometheus/cmd/promtool`

## Generate config files

You can manually generate the alerts, dashboards and rules files:

```
$ make prometheus_alerts.yaml
$ make prometheus_rules.yaml
$ make dashboards_out
```

The `prometheus_alerts.yaml` and `prometheus_rules.yaml` file then need to passed
to your Prometheus server, and the files in `dashboards_out` need to be imported
into you Grafana server. The exact details will depending on how you deploy your
monitoring stack.

## Running the tests (requires Docker)

Build the mixins, run the tests:

```
$ docker run -v $(pwd):/tmp --entrypoint "/bin/promtool" prom/prometheus:latest test rules /tmp/tests.yaml
```

Generate the alerts, rules and dashboards:

```
$ jsonnet -J vendor -S -e 'std.manifestYamlDoc((import "mixin.libsonnet").prometheusAlerts)' > alerts.yml
$ jsonnet -J vendor -S -e 'std.manifestYamlDoc((import "mixin.libsonnet").prometheusRules)' >files/rules.yml
$ jsonnet -J vendor -m files/dashboards -e '(import "mixin.libsonnet").grafanaDashboards'
```

## Background

* For more motivation, see
"[The RED Method: How to instrument your services](https://kccncna17.sched.com/event/CU8K/the-red-method-how-to-instrument-your-services-b-tom-wilkie-kausal?iframe=no&w=100%&sidebar=yes&bg=no)" talk from CloudNativeCon Austin.
* For more information about monitoring mixins, see this [design doc](https://docs.google.com/document/d/1A9xvzwqnFVSOZ5fD3blKODXfsat5fg6ZhnKu9LK3lB4/edit#).
