<?php
namespace Vitess;

interface RpcClient
{

    public function execute(Context $ctx, Proto\Vtgate\ExecuteRequest $request);

    public function executeShards(Context $ctx, Proto\Vtgate\ExecuteShardsRequest $request);

    public function executeKeyspaceIds(Context $ctx, Proto\Vtgate\ExecuteKeyspaceIdsRequest $request);

    public function executeKeyRanges(Context $ctx, Proto\Vtgate\ExecuteKeyRangesRequest $request);

    public function executeEntityIds(Context $ctx, Proto\Vtgate\ExecuteEntityIdsRequest $request);

    public function executeBatchShards(Context $ctx, Proto\Vtgate\ExecuteBatchShardsRequest $request);

    public function executeBatchKeyspaceIds(Context $ctx, Proto\Vtgate\ExecuteBatchKeyspaceIdsRequest $request);

    public function streamExecute(Context $ctx, Proto\Vtgate\StreamExecuteRequest $request);

    public function streamExecuteShards(Context $ctx, Proto\Vtgate\StreamExecuteShardsRequest $request);

    public function streamExecuteKeyspaceIds(Context $ctx, Proto\Vtgate\StreamExecuteKeyspaceIdsRequest $request);

    public function streamExecuteKeyRanges(Context $ctx, Proto\Vtgate\StreamExecuteKeyRangesRequest $request);

    public function begin(Context $ctx, Proto\Vtgate\BeginRequest $request);

    public function commit(Context $ctx, Proto\Vtgate\CommitRequest $request);

    public function rollback(Context $ctx, Proto\Vtgate\RollbackRequest $request);

    public function getSrvKeyspace(Context $ctx, Proto\Vtgate\GetSrvKeyspaceRequest $request);

    public function splitQuery(Context $ctx, Proto\Vtgate\SplitQueryRequest $request);

    public function close();
}
