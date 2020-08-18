FROM golang:1.14
RUN go get -v github.com/cespare/reflex
COPY reflex.conf /reflex.conf
COPY entrypoint.sh /entrypoint.sh
COPY endpoints.yaml /endpoints.yaml
ENTRYPOINT ["/entrypoint.sh"]
