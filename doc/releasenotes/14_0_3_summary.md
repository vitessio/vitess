## Known Issues

- [Corrupted results for non-full-group-by queries with JOINs](https://github.com/vitessio/vitess/issues/11625). This can be resolved by using full-group-by queries.

## Major Changes

### Fix VTOrc Discovery

In patch releases prior to release 14.0.3, if VTOrc is unable to reach the MySQL instance of a vttablet, it was never able to read discover that tablet again.
This problem could be resolved by restarting the VTOrc so that it discovers all the tablets again, but in a kubernetes cluster where the pods are eviced 
frequently, this posed a greater challenge, since some pods when evicted and rescheduled on a different node, would sometimes fail to be discovered by VTOrc.
This has problem has been addressed in this patch by the fix https://github.com/vitessio/vitess/pull/10662.