# Use a [multi stage
# build](https://docs.docker.com/develop/develop-images/multistage-build/) to
# build [reflex](https://github.com/cespare/reflex), a tool that will allow us
# to automatically rerun the project when any files change.
FROM golang:1.12.5 AS build
# Build reflex as a static binary (CGO_ENABLED=0) so we can run it in our final
# container.
RUN CGO_ENABLED=0 go get -v github.com/cespare/reflex

FROM golang:1.12.5-alpine AS runtime
COPY --from=build /go/bin/reflex /go/bin/reflex
COPY reflex.conf /
ENTRYPOINT ["/go/bin/reflex", "-c", "/reflex.conf"]
