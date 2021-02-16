# Vitess Mixin End to End Testing

Vitess Mixin uses [Cypress](https://www.cypress.io).

## Purpose

These tests attempt to assert truth to the following:

* Does the library generate valid dashboards JSON?
* Are dashboard elements displayed as expected?
* Do elements get configured as intended?
* Do the configured elements do what they're expected to do?

Some of this is automated here. However, the visual aspects are difficult for
machines to cover. Even some behavioral aspects are as well because they incur
an impractical amount of complexity, time, or cost. For those aspects, these
tests provide a way to quickly generate dashboards consistently so we can use
our human abilities to assert truth.

## Usage

`docker-compose` is used to run Cypress and Grafana. There are two targets in
[Makefile](../Makefile) to help run it.

`make e2e`: runs tests headless and exits.

`make e2e-dev`: opens the [the test
runner](https://docs.cypress.io/guides/core-concepts/test-runner.html#Overview)
and exposes Grafana to the host machine - [http://localhost:3030](http://localhost:3030). This requires
an X11 server to work. [This
post](https://www.cypress.io/blog/2019/05/02/run-cypress-with-a-single-docker-command/#Interactive-mode)
describes how to set this up with [XQuartz](https://www.xquartz.org/).

## Notes

Tests depend on compiled artifacts in [dashaboards_out](../dashboards_out) for generating
dashboards.
