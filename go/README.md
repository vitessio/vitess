This directory contains all the Go code for Vitess.

Most of the packages at the top level are general-purpose and are suitable
for use outside Vitess. Packages that are specific to Vitess are in the *vt*
subdirectory. Binaries are in the *cmd* subdirectory.

Please see [GoDoc](https://godoc.org/vitess.io/vitess/go) for
a listing of the packages and their purposes.

vt/proto contains the compiled protos for go, one per each directory.
When importing these protos (for instance XXX.proto), we rename them on
import to XXXpb. For instance:

```go
import (
    topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)
```

