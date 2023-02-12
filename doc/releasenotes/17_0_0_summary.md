## Summary

## Major Changes

### Breaking Changes

#### Dedicated stats for VTGate Prepare operations

Previously Vitess did not generate independent stats for VTGate Execute and Prepare operations. The two operations shared the same stat key (`Execute`). In v17 Prepare operations generate independent stats.

For example:

```
{
  "VtgateApi": {
    "TotalCount": 27,
    "TotalTime": 468068832,
    "Histograms": {
      "Execute.src.primary": {
        "500000": 5,
        "1000000": 0,
        "5000000": 4,
        "10000000": 6,
        "50000000": 6,
        "100000000": 0,
        "500000000": 1,
        "1000000000": 0,
        "5000000000": 0,
        "10000000000": 0,
        "inf": 0,
        "Count": 22,
        "Time": 429444291
      },
      "Prepare.src.primary": {
        "500000": 0,
        "1000000": 0,
        "5000000": 3,
        "10000000": 1,
        "50000000": 1,
        "100000000": 0,
        "500000000": 0,
        "1000000000": 0,
        "5000000000": 0,
        "10000000000": 0,
        "inf": 0,
        "Count": 5,
        "Time": 38624541
      }
    }
  },
  "VtgateApiErrorCounts": {
    "Prepare.src.primary.NOT_FOUND": 1,
    "Execute.src.primary.INVALID_ARGUMENT": 3,
    "Execute.src.primary.ALREADY_EXISTS": 1
  }
}
```
