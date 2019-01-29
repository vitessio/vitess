# Vitess Monitoring

This page explains the current state of Vitess metrics monitoring, and potential future work in the area.

## Current state of monitoring

There are currently three main ways that a Vitess cluster can be monitored. Depending on your needs, you can use any of the following methods:

### 1. Vitess status pages

The status HTML pages of various Vitess components can be accessed by pointing your browser to `http://<host>:<port>/debug/status`. The status pages will often display some basic, but useful, information for monitoring. For example, the status page of a vttablet will show the QPS graph for the past few minutes.

Viewing a status page can be useful since it works out of the box, but it only provides very basic monitoring capabilities.

### 2. Pull-based metrics system

Vitess uses Go’s [expvar package](http://golang.org/pkg/expvar/) to expose various metrics, with the expectation that a user can configure a pull-based metrics system to ingest those metrics. Metrics are published to `http://<host>:<port>/debug/vars` as JSON key-value pairs, which should be easy for any metrics system to parse.

Scraping Vitess variables is a good way to integrate Vitess into an existing monitoring system, and is useful for building up detailed monitoring dashboards. It is also the officially supported way for monitoring Vitess.

### 3. Push-based metrics system

Vitess also includes support for push-based metrics systems via plug-ins. Each Vitess component would need to be run with the `--emit_stats` flag.

By default, the stats_emit_period is 60s, so each component will push stats to the selected backend every minute. This is configurable via the `--stats_emit_period` flag.

Vitess has preliminary plug-ins to support OpenTSDB as a push-based metrics backend.

It should be fairly straightforward to write your own plug-in, if you want to support a different backend. The plug-in package simply needs to implement the `PushBackend` interface of the `stats` package. For an example, you can see the [OpenTSDB plugin](https://github.com/vitessio/vitess/blob/master/go/stats/opentsdb/opentsdb.go).

Once you’ve written the backend plug-in, you also need to register the plug-in from within all the relevant Vitess binaries. An example of how to do this can be seen in [this pull request](https://github.com/vitessio/vitess/pull/469).

You can then specify that Vitess should publish stats to the backend that you’re targeting by using the `--stats_backend` flag.  

Connecting Vitess to a push-based metrics system can be useful if you’re already running a push-based system that you would like to integrate into. More discussion on using a push vs pull based monitoring system can be seen here: [http://www.boxever.com/push-vs-pull-for-monitoring](http://www.boxever.com/push-vs-pull-for-monitoring)

## Monitoring with Kubernetes

The existing methods for integrating metrics are not supported in a Kubernetes environment by the Vitess team yet, but are on the roadmap for the future. However, it should be possible to get the Prometheus backend working with Kubernetes, similar to how [Heapster for Kubernetes works](https://github.com/kubernetes/kubernetes/tree/master/cluster/addons/prometheus). 

In the meantime, if you run into issues or have questions, please post on our [forum](https://groups.google.com/forum/#!forum/vitess).


