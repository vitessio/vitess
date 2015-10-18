<?php
require_once (dirname(__FILE__) . '/VTProto.php');
require_once (dirname(__FILE__) . '/VTContext.php');
require_once (dirname(__FILE__) . '/VTCursor.php');
require_once (dirname(__FILE__) . '/VTGateTx.php');

class VTGateConn {
	protected $client;

	public function __construct($client) {
		$this->client = $client;
	}

	public function execute(VTContext $ctx, $query, array $bind_vars, $tablet_type) {
		$request = new \vtgate\ExecuteRequest();
		$request->setQuery(VTProto::BoundQuery($query, $bind_vars));
		$request->setTabletType($tablet_type);
		if ($ctx->getCallerId()) {
			$request->setCallerId($ctx->getCallerId());
		}
		$response = $this->client->execute($ctx, $request);
		VTProto::checkError($response);
		return new VTCursor($response->getResult());
	}

	public function executeShards(VTContext $ctx, $query, $keyspace, array $shards, array $bind_vars, $tablet_type) {
		$request = new \vtgate\ExecuteShardsRequest();
		$request->setQuery(VTProto::BoundQuery($query, $bind_vars));
		$request->setTabletType($tablet_type);
		$request->setKeyspace($keyspace);
		$request->setShards($shards);
		if ($ctx->getCallerId()) {
			$request->setCallerId($ctx->getCallerId());
		}
		$response = $this->client->executeShards($ctx, $request);
		VTProto::checkError($response);
		return new VTCursor($response->getResult());
	}

	public function executeKeyspaceIds(VTContext $ctx, $query, $keyspace, array $keyspace_ids, array $bind_vars, $tablet_type) {
		$request = new \vtgate\ExecuteKeyspaceIdsRequest();
		$request->setQuery(VTProto::BoundQuery($query, $bind_vars));
		$request->setTabletType($tablet_type);
		$request->setKeyspace($keyspace);
		$request->setKeyspaceIds($keyspace_ids);
		if ($ctx->getCallerId()) {
			$request->setCallerId($ctx->getCallerId());
		}
		$response = $this->client->executeKeyspaceIds($ctx, $request);
		VTProto::checkError($response);
		return new VTCursor($response->getResult());
	}

	public function executeKeyRanges(VTContext $ctx, $query, $keyspace, array $key_ranges, array $bind_vars, $tablet_type) {
		$request = new \vtgate\ExecuteKeyRangesRequest();
		$request->setQuery(VTProto::BoundQuery($query, $bind_vars));
		$request->setTabletType($tablet_type);
		$request->setKeyspace($keyspace);
		VTProto::addKeyRanges($request, $key_ranges);
		if ($ctx->getCallerId()) {
			$request->setCallerId($ctx->getCallerId());
		}
		$response = $this->client->executeKeyRanges($ctx, $request);
		VTProto::checkError($response);
		return new VTCursor($response->getResult());
	}

	public function executeEntityIds(VTContext $ctx, $query, $keyspace, $entity_column_name, array $entity_keyspace_ids, array $bind_vars, $tablet_type) {
		$request = new \vtgate\ExecuteEntityIdsRequest();
		$request->setQuery(VTProto::BoundQuery($query, $bind_vars));
		$request->setTabletType($tablet_type);
		$request->setKeyspace($keyspace);
		$request->setEntityColumnName($entity_column_name);
		VTProto::addEntityKeyspaceIds($request, $entity_keyspace_ids);
		if ($ctx->getCallerId()) {
			$request->setCallerId($ctx->getCallerId());
		}
		$response = $this->client->executeEntityIds($ctx, $request);
		VTProto::checkError($response);
		return new VTCursor($response->getResult());
	}

	public function executeBatchShards(VTContext $ctx, array $bound_shard_queries, $tablet_type, $as_transaction) {
		$request = new \vtgate\ExecuteBatchShardsRequest();
		VTProto::addQueries($request, $bound_shard_queries);
		$request->setTabletType($tablet_type);
		$request->setAsTransaction($as_transaction);
		if ($ctx->getCallerId()) {
			$request->setCallerId($ctx->getCallerId());
		}
		$response = $this->client->executeBatchShards($ctx, $request);
		VTProto::checkError($response);
		$results = array();
		foreach ($response->getResultsList() as $result) {
			$results[] = new VTCursor($result);
		}
		return $results;
	}

	public function executeBatchKeyspaceIds(VTContext $ctx, array $bound_keyspace_id_queries, $tablet_type, $as_transaction) {
		$request = new \vtgate\ExecuteBatchKeyspaceIdsRequest();
		VTProto::addQueries($request, $bound_keyspace_id_queries);
		$request->setTabletType($tablet_type);
		$request->setAsTransaction($as_transaction);
		if ($ctx->getCallerId()) {
			$request->setCallerId($ctx->getCallerId());
		}
		$response = $this->client->executeBatchKeyspaceIds($ctx, $request);
		VTProto::checkError($response);
		$results = array();
		foreach ($response->getResultsList() as $result) {
			$results[] = new VTCursor($result);
		}
		return $results;
	}

	public function streamExecute(VTContext $ctx, $query, array $bind_vars, $tablet_type) {
		$request = new \vtgate\StreamExecuteRequest();
		$request->setQuery(VTProto::BoundQuery($query, $bind_vars));
		$request->setTabletType($tablet_type);
		if ($ctx->getCallerId()) {
			$request->setCallerId($ctx->getCallerId());
		}
		$call = $this->client->streamExecute($ctx, $request);
		return new VTStreamCursor($call);
	}

	public function streamExecuteShards(VTContext $ctx, $query, $keyspace, array $shards, array $bind_vars, $tablet_type) {
		$request = new \vtgate\StreamExecuteShardsRequest();
		$request->setQuery(VTProto::BoundQuery($query, $bind_vars));
		$request->setKeyspace($keyspace);
		$request->setShards($shards);
		$request->setTabletType($tablet_type);
		if ($ctx->getCallerId()) {
			$request->setCallerId($ctx->getCallerId());
		}
		$call = $this->client->streamExecuteShards($ctx, $request);
		return new VTStreamCursor($call);
	}

	public function streamExecuteKeyspaceIds(VTContext $ctx, $query, $keyspace, array $keyspace_ids, array $bind_vars, $tablet_type) {
		$request = new \vtgate\StreamExecuteKeyspaceIdsRequest();
		$request->setQuery(VTProto::BoundQuery($query, $bind_vars));
		$request->setKeyspace($keyspace);
		$request->setKeyspaceIds($keyspace_ids);
		$request->setTabletType($tablet_type);
		if ($ctx->getCallerId()) {
			$request->setCallerId($ctx->getCallerId());
		}
		$call = $this->client->streamExecuteKeyspaceIds($ctx, $request);
		return new VTStreamCursor($call);
	}

	public function streamExecuteKeyRanges(VTContext $ctx, $query, $keyspace, array $key_ranges, array $bind_vars, $tablet_type) {
		$request = new \vtgate\StreamExecuteKeyRangesRequest();
		$request->setQuery(VTProto::BoundQuery($query, $bind_vars));
		$request->setKeyspace($keyspace);
		VTProto::addKeyRanges($request, $key_ranges);
		$request->setTabletType($tablet_type);
		if ($ctx->getCallerId()) {
			$request->setCallerId($ctx->getCallerId());
		}
		$call = $this->client->streamExecuteKeyRanges($ctx, $request);
		return new VTStreamCursor($call);
	}

	public function begin(VTContext $ctx) {
		$request = new \vtgate\BeginRequest();
		if ($ctx->getCallerId()) {
			$request->setCallerId($ctx->getCallerId());
		}
		
		$response = $this->client->begin($ctx, $request);
		return new VTGateTx($this->client, $response->getSession());
	}

	public function splitQuery(VTContext $ctx, $keyspace, $query, array $bind_vars, $split_column, $split_count) {
		$request = new \vtgate\SplitQueryRequest();
		$request->setKeyspace($keyspace);
		$request->setQuery(VTProto::BoundQuery($query, $bind_vars));
		$request->setSplitColumn($split_column);
		$request->setSplitCount($split_count);
		if ($ctx->getCallerId()) {
			$request->setCallerId($ctx->getCallerId());
		}
		
		$response = $this->client->splitQuery($ctx, $request);
		return $response->getSplitsList();
	}

	public function getSrvKeyspace(VTContext $ctx, $keyspace) {
		$request = new \vtgate\GetSrvKeyspaceRequest();
		$request->setKeyspace($keyspace);
		$response = $this->client->getSrvKeyspace($ctx, $request);
		return $response->getSrvKeyspace();
	}

	public function close() {
		$this->client->close();
	}
}
