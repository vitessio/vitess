# (Beta) Monitoring Mixin for Vitess

A set of Grafana dashboards, Prometheus rules and alerts for Vitess, packaged together in a reusable and extensible bundle.

## 🔁 Prerequisites

1. Go (programming language)
    - Install binaries using the official [installer](https://golang.org/dl/)
    - Ensure `GOPATH` variable is set in your system. See instructions [here](https://golang.org/doc/install#install). Here below there's a sample config:

        ```shell
        export GOPATH=$HOME/go
        export PATH="$GOPATH/bin:/usr/local/go/bin:$PATH"
        ```

1. Install the go tools: `make tools`, `jb`, `jsonnet`, `jsonnetfmt`, and `promtool` should now be in `$GOPATH/bin`.

1. Install the dependencies by running: `jb install`

## ℹ️ How-to

Customize `config.libsonnet` based on your setup. Example: specify the `dataSource` name (default to `Prometheus_Vitess`). You can then generate:

- Prometheus alerts: `$ make prometheus_alerts.yaml`
(Note: This files is empty because the current version of the mixin uses Grafana Alerts)

- Prometheus rules: `$ make prometheus_rules.yaml`

- Grafana dashboard: `$ ENV='prod' make dashboards_out` (Supported environments are `dev` and `prod`).

The `prometheus_alerts.yaml` and `prometheus_rules.yaml` file then need to passed to your Prometheus server, and the files in `dashboards_out` need to be imported into you Grafana server.

## 👩‍💻 Development

If you want to contribute please read [Vitess mixin quickstart guide](vitess-mixin-quickstart.md)

## 📚 Useful links & further learning

- For more information about monitoring mixins, see this [design doc](https://docs.google.com/document/d/1A9xvzwqnFVSOZ5fD3blKODXfsat5fg6ZhnKu9LK3lB4/edit#).
- For more motivation, see
"[The RED Method: How to instrument your services](https://kccncna17.sched.com/event/CU8K/the-red-method-how-to-instrument-your-services-b-tom-wilkie-kausal?iframe=no&w=100%&sidebar=yes&bg=no)" talk from CloudNativeCon Austin.
