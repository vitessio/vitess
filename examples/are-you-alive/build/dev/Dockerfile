FROM golang:1.14
RUN go install github.com/cespare/reflex@latest
COPY reflex.conf /reflex.conf
COPY entrypoint.sh /entrypoint.sh
COPY endpoints.yaml /endpoints.yaml
ENTRYPOINT ["/entrypoint.sh"]
