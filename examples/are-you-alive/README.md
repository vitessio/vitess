# Are You Alive?

What does it mean to be alive?

Well we don't know what it means for you, but we know what it means for a Vitess
Cluster!

This project contains a simulated client application that can be used to measure
the health of a Vitess cluster over time.

## Design

For now, there is a specific database schema and vschema that you must apply to
the database that you are using for this test.

This client application:

1. Hammers the database with random data (not a load test though).
1. Measures all the important things:
   - Client connection errors
   - Write latency
   - Read latency from masters
   - Read latency from replicas
   - Write errors
   - Read errors on masters
   - Write errors on replicas
   - Errors in other operations on masters and replicas (e.g. COUNT)
   - Latency on other operations on masters and replicas (e.g. COUNT)
   - Data loss (by writing predictable data and testing for that)
1. Reports all these metrics to Prometheus.

That's it!

## Usage

First, [initialize your database with the correct schemas](schemas/README.md).

Run `are-you-alive --help` for usage.  You can us the command line flags to
control the dataset size, whether to target reads at masters and replicas, your
mysql connection string, and the rate at which to send requests.

Example:

```
./are-you-alive --mysql_connection_string <mysql_connection_string>
```

Where `<mysql_connection_string>` points to the database you are trying to test,
and everything else will be set to defaults.

## Building

```
go build vitess.io/vitess/examples/are-you-alive/cmd/are-you-alive
```

## Testing

First, [install docker compose](https://docs.docker.com/compose/install/) and
make sure it's working.  Then run:

```
docker-compose build
docker-compose up
```

This will create a local mysqld and a local prometheus to scrape the app.  It
will also start the app with the `--initialize` flag which tells it to
automatically create the test database.  You might have to run this twice to
give mysql a chance to do its first initialization.

After you run docker compose, navigate to `http://localhost:9090` to see
Prometheus and `http://localhost:8080/metrics` to see the raw metrics being
exported.

## Test Specific Tablet Types

See [this vitess
documentation](https://vitess.io/docs/user-guides/faq/#how-do-i-choose-between-master-vs-replica-for-queries)
for how to target specific tablet types. In the configuration file you'll want
to, for example, put "@master" or "@replica" on the ends of your connection
strings.

## Push to Registry

If you have push access to the [planetscale public
registry](https://us.gcr.io/planetscale-vitess), you can use the following
commands to build and push the image:

```
make build
make push
```
