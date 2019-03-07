# vitess/guestbook

This is a Docker image for a sample guestbook app that uses Vitess.

It is essentially a port of the
[kubernetes/guestbook-go](https://github.com/kubernetes/examples/tree/master/guestbook-go)
example, but using Python instead of Go for the app server,
and Vitess instead of Redis for the storage engine.

It has also been modified to support multiple Guestbook pages,
to better demonstrate sharding support in Vitess.

Note that the Dockerfile should be built with the accompanying build.sh script.
See the comments in the script for more information.
