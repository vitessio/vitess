<?php
require_once (dirname(__FILE__) . '/proto/vtgate.php');
require_once (dirname(__FILE__) . '/VTContext.php');

interface VTRpcClient {

	public function execute(VTContext $ctx, \vtgate\ExecuteRequest $request);

	public function executeShards(VTContext $ctx, \vtgate\ExecuteShardsRequest $request);

	public function executeKeyspaceIds(VTContext $ctx, \vtgate\ExecuteKeyspaceIdsRequest $request);

	public function executeKeyRanges(VTContext $ctx, \vtgate\ExecuteKeyRangesRequest $request);

	public function executeEntityIds(VTContext $ctx, \vtgate\ExecuteEntityIdsRequest $request);

	public function executeBatchShards(VTContext $ctx, \vtgate\ExecuteBatchShardsRequest $request);

	public function executeBatchKeyspaceIds(VTContext $ctx, \vtgate\ExecuteBatchKeyspaceIdsRequest $request);

	public function streamExecute(VTContext $ctx, \vtgate\StreamExecuteRequest $request);

	public function streamExecuteShards(VTContext $ctx, \vtgate\StreamExecuteShardsRequest $request);

	public function streamExecuteKeyspaceIds(VTContext $ctx, \vtgate\StreamExecuteKeyspaceIdsRequest $request);

	public function streamExecuteKeyRanges(VTContext $ctx, \vtgate\StreamExecuteKeyRangesRequest $request);

	public function begin(VTContext $ctx, \vtgate\BeginRequest $request);

	public function commit(VTContext $ctx, \vtgate\CommitRequest $request);

	public function rollback(VTContext $ctx, \vtgate\RollbackRequest $request);

	public function getSrvKeyspace(VTContext $ctx, \vtgate\GetSrvKeyspaceRequest $request);

	public function splitQuery(VTContext $ctx, \vtgate\SplitQueryRequest $request);

	public function close();
}
