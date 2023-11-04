# VTAdmin

VTAdmin is web UI and API that allows users to manage multiple Vitess clusters at once.

For a more detailed writeup, refer to the [original RFC](https://github.com/vitessio/vitess/issues/7117).

## Setup

The simplest VTAdmin deployment involves a single Vitess cluster. You can look
at the [local example](../../../examples/local/scripts/vtadmin-up.sh) for a
minimal invocation of the `vtadmin-api` and `vtadmin-web` binaries.

### Important `vtadmin-api` flags

Please refer to `vtadmin --help` for the full listing, but a few flags warrant
explanation here.

* `--http-origin` — this flag sets up the allowed CORS origins that `vtadmin-api`
  will serve HTTP requests for, and is required if you are (very likely) running
  `vtadmin-api` and `vtadmin-web` on different domains.
* `--cluster`, `--cluster-defaults` — A [DSN-style][dsn] flag that allows cluster
  configuration options to be specified on the command-line rather than needing
  a config file. When both command-line cluster configs and a config file are
  provided, any options for a given cluster on the command-line take precedence
  over options for that cluster in the config file.

  For a description of the cluster configuration options, see [clusters.example.yaml](../../../doc/vtadmin/clusters.yaml).

* `--http-tablet-url-tmpl` — Go template string to generate a reachable http(s)
  address for a tablet, used to make passthrough requests to `/debug/vars`
  endpoints.

[dsn]: https://www.percona.com/doc/percona-toolkit/LATEST/dsn_data_source_name_specifications.html

## Development

### Building `vtadmin-api`

If you are making changes to `.proto` files, make sure you run

```
source dev.env
make proto grpcvtctldclient vtadmin_web_proto_types
```

Then, you can run `make build`, and run `./bin/vtadmin` with any flags you need
(see the local example, and also the section on flags above).

### Building and running `vtadmin-web`

Make sure you are using node version 16.x.

Then, you may run:

```
cd ./web/vtadmin
npm install

# This should be the address you passed to `./vtadmin --addr`. For example,
# "http://127.0.0.1:14200".
export VITE_VTADMIN_API_ADDRESS="${vtadmin_api_addr}"
export VITE_ENABLE_EXPERIMENTAL_TABLET_DEBUG_VARS="true"
npm run start
```
