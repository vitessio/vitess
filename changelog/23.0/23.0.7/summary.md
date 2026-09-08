# Release of Vitess v23.0.7

## Summary

### lz4 backup engine: library upgrade and `--compression-level` mapping

The `lz4` compression engine now uses the `pierrec/lz4/v4` library instead of `pierrec/lz4` v2. This fixes broken block decoding on amd64 in the v2 library. The frame format is unchanged, so backups written by older Vitess versions remain restorable and backups written by this version are restorable by older versions.

The upgrade changes how `--compression-level` is interpreted for the lz4 engine. Values `0` and `1`, including the default of `1`, select the fast compressor. Values `2` through `9` now select lz4's named hash-chain levels (`Level2` through `Level9`) instead of using the raw value as the hash-chain search depth, so higher values produce a better ratio at more CPU cost. Values above `9` and negative values, which previously requested an unlimited search, select `Level9`. Other compression engines are not affected.

See [#20778](https://github.com/vitessio/vitess/pull/20778) for details.
