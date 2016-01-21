<?php
require_once (dirname(__FILE__) . '/proto/vtgate.php');
require_once (dirname(__FILE__) . '/VTContext.php');

interface VTRpcClient {

	public function execute(VTContext $ctx, Proto\Vtgate\ExecuteRequest $request);

	public function executeShards(VTContext $ctx, Proto\Vtgate\ExecuteShardsRequest $request);

	public function executeKeyspaceIds(VTContext $ctx, Proto\Vtgate\ExecuteKeyspaceIdsRequest $request);

	public function executeKeyRanges(VTContext $ctx, Proto\Vtgate\ExecuteKeyRangesRequest $request);

	public function executeEntityIds(VTContext $ctx, Proto\Vtgate\ExecuteEntityIdsRequest $request);

	public function executeBatchShards(VTContext $ctx, Proto\Vtgate\ExecuteBatchShardsRequest $request);

	public function executeBatchKeyspaceIds(VTContext $ctx, Proto\Vtgate\ExecuteBatchKeyspaceIdsRequest $request);

	public function streamExecute(VTContext $ctx, Proto\Vtgate\StreamExecuteRequest $request);

	public function streamExecuteShards(VTContext $ctx, Proto\Vtgate\StreamExecuteShardsRequest $request);

	public function streamExecuteKeyspaceIds(VTContext $ctx, Proto\Vtgate\StreamExecuteKeyspaceIdsRequest $request);

	public function streamExecuteKeyRanges(VTContext $ctx, Proto\Vtgate\StreamExecuteKeyRangesRequest $request);

	public function begin(VTContext $ctx, Proto\Vtgate\BeginRequest $request);

	public function commit(VTContext $ctx, Proto\Vtgate\CommitRequest $request);

	public function rollback(VTContext $ctx, Proto\Vtgate\RollbackRequest $request);

	public function getSrvKeyspace(VTContext $ctx, Proto\Vtgate\GetSrvKeyspaceRequest $request);

	public function splitQuery(VTContext $ctx, Proto\Vtgate\SplitQueryRequest $request);

	public function close();
}
