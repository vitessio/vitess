## Major Changes

### Command-line syntax deprecations

### New Syntax

### VDiff2

We introduced the ability to resume a VDiff2 workflow:
```
$ vtctlclient --server=localhost:15999 VDiff -- --v2 customer.commerce2customer resume 4c664dc2-eba9-11ec-9ef7-920702940ee0
VDiff 4c664dc2-eba9-11ec-9ef7-920702940ee0 resumed on target shards, use show to view progress

$ vtctlclient --server=localhost:15999 VDiff -- --v2 customer.commerce2customer show last

VDiff Summary for customer.commerce2customer (4c664dc2-eba9-11ec-9ef7-920702940ee0)
State: completed
RowsCompared: 196
CompletedAt:  2022-06-17 14:37:25
HasMismatch:  false

Use "--format=json" for more detailed output.

$ vtctlclient --server=localhost:15999 VDiff -- --v2 --format=json customer.commerce2customer show last
{
	"Workflow": "commerce2customer",
	"Keyspace": "customer",
	"State": "completed",
	"UUID": "4c664dc2-eba9-11ec-9ef7-920702940ee0",
	"RowsCompared": 196,
	"HasMismatch": false,
	"Shards": "0",
	"CompletedAt": "2022-06-17 14:37:25"
}
```

Please see the VDiff2 [documentation](https://vitess.io/docs/15.0/reference/vreplication/vdiff2/) for additional information.

### New command line flags and behavior
