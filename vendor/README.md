# govendor

We manage the file `vendor.json` through the [govendor](https://github.com/kardianos/govendor) command.

## Add a new dependency

```sh
govendor fetch <package_path>@<version>
```

If available, please always use a release version. If not, you can omit `@<version>`.

## Update a dependency

Example gRPC:

```sh
govendor fetch google.golang.org/grpc/...@v1.11.2
```
