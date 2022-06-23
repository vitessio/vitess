## Major Changes

### Command-line syntax deprecations

### New command line flags and behavior

### Online DDL changes

### Tablet throttler

#### API changes

API endpoint `/debug/vars` now exposes throttler metrics, such as number of hits and errors per app per check type. Example:

```shell
$ curl -s 'http://127.0.0.1:15100/debug/vars' | jq . | grep throttler
  "throttler.aggregated.mysql.self": 133.19334,
  "throttler.aggregated.mysql.shard": 132.997847,
  "throttler.check.any.error": 1086,
  "throttler.check.any.mysql.self.error": 542,
  "throttler.check.any.mysql.self.total": 570,
  "throttler.check.any.mysql.shard.error": 544,
  "throttler.check.any.mysql.shard.total": 570,
  "throttler.check.any.total": 1140,
  "throttler.check.mysql.self.seconds_since_healthy": 132,
  "throttler.check.mysql.shard.seconds_since_healthy": 132,
  "throttler.check.vitess.error": 1086,
  "throttler.check.vitess.mysql.self.error": 542,
  "throttler.check.vitess.mysql.self.total": 570,
  "throttler.check.vitess.mysql.shard.error": 544,
  "throttler.check.vitess.mysql.shard.total": 570,
  "throttler.check.vitess.total": 1140,
  "throttler.probes.latency": 292982,
  "throttler.probes.total": 1138
```
