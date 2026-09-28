# VTTablet with RBR mode

The deprecation of SBR will result in a fundamental change in how vttablet works. The most significant one is that vttablet does not need to compute which rows will be affected by a DML. Dropping this requirement allows us to rethink the implementation of its various features. Some of these changes will not be fully backward compatible. Here are the details:

## Pass-through DMLs

Most DMLs can be just passed through.

There is, however, one valuable feature that we want to preserve: the ability to limit the number of rows a DML affects. In SBR mode, this was achieved as a side-effect of the fact that we had to run a “subquery” to identify all affected rows, which allowed us to count them and return an error if they exceeded the limit. We lose this ability in pass-through mode.

Instead of continuing to issue the subquery, the new proposal is to add a LIMIT clause to the DML itself. Then, if “RowsAffected” was greater than the limit, we return an error. The one downside of this implementation is that such a failure will leave the DML partially executed. This means that vttablet will have to force a rollback of the transaction. In most use cases, the application is expected to rollback the transaction on such an error. Therefore, this sounds like a reasonable trade-off for the level of simplicity achieved. There is also the added benefit of avoiding the extra roundtrip for the subquery.

This change has a subtle interaction with the “found rows” flag, which reports affected rows differently. This just means that users of this flag may have to set their limits differently.

The explicit pass-through mode flag `queryserver-config-passthrough-dmls` will continue to behave as is. In the new context, its meaning will change to: “do not limit affected rows”.

We believe that inserts of a large number of rows are usually intentional. Therefore, limits will only be applied to updates and deletes.

## Autocommits

With most DMLs being pass-through, we can now resurrect autocommit behavior in vttablet. This means that a transactionless DML can be treated as autocommit. In the case where a limit has to be enforced, vttablet can open a transaction, and then rollback if the limit is exceeded.

## Sequences

Sequences will continue their existing behavior: normal statements will continue to be sent to mysql. If a `select next...` is received, the sequence specific functionality will be triggered.

## Messages

Messages are undergoing a significant overhaul as seen in #5913. Tracking of row changes has been changed to use VStreamer, which allows for any DML to be executed on those tables. Inserts remain an exception because columns like `time_created` and `time_scheduled` have to be populated by vttablet in order to maintain backward compatibility.

## Plan IDs

Most plan ids were specific to SBR. After the refactor, the total number of plans will be greatly reduced, and we’ll likely generate a new set of ids.

## Schema

These changes greatly reduce our dependence on the schema. We only need to know the following info from a table:

* Field info
* PK columns
* Table comments

VTTablet also gathered table statistics from `information schema`. However, we don’t think anyone is using them. So, it may be better to stop reporting them. If anyone still needs them, please speak up now.
