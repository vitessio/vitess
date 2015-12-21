<?php

class VTGateTx {
	protected $client;
	protected $session;

	public function __construct($client, $session) {
		$this->client = $client;
		$this->session = $session;
	}

	private function inTransaction() {
		return ! is_null($this->session) && $this->session->getInTransaction();
	}

	public function execute(VTContext $ctx, $query, array $bind_vars, $tablet_type = \topodata\TabletType::MASTER, $not_in_transaction = FALSE) {
		if (! $this->inTransaction()) {
			throw new VTException('execute called while not in transaction.');
		}
		
		$request = new \vtgate\ExecuteRequest();
		$request->setSession($this->session);
		$request->setQuery(VTProto::BoundQuery($query, $bind_vars));
		$request->setTabletType($tablet_type);
		$request->setNotInTransaction($not_in_transaction);
		if ($ctx->getCallerId()) {
			$request->setCallerId($ctx->getCallerId());
		}
		$response = $this->client->execute($ctx, $request);
		$this->session = $response->getSession();
		VTProto::checkError($response);
		return new VTCursor($response->getResult());
	}

	public function executeShards(VTContext $ctx, $query, $keyspace, array $shards, array $bind_vars, $tablet_type = \topodata\TabletType::MASTER, $not_in_transaction = FALSE) {
		if (! $this->inTransaction()) {
			throw new VTException('execute called while not in transaction.');
		}
		
		$request = new \vtgate\ExecuteShardsRequest();
		$request->setSession($this->session);
		$request->setQuery(VTProto::BoundQuery($query, $bind_vars));
		$request->setTabletType($tablet_type);
		$request->setNotInTransaction($not_in_transaction);
		$request->setKeyspace($keyspace);
		$request->setShards($shards);
		if ($ctx->getCallerId()) {
			$request->setCallerId($ctx->getCallerId());
		}
		$response = $this->client->executeShards($ctx, $request);
		$this->session = $response->getSession();
		VTProto::checkError($response);
		return new VTCursor($response->getResult());
	}

	public function executeKeyspaceIds(VTContext $ctx, $query, $keyspace, array $keyspace_ids, array $bind_vars, $tablet_type = \topodata\TabletType::MASTER, $not_in_transaction = FALSE) {
		if (! $this->inTransaction()) {
			throw new VTException('execute called while not in transaction.');
		}
		
		$request = new \vtgate\ExecuteKeyspaceIdsRequest();
		$request->setSession($this->session);
		$request->setQuery(VTProto::BoundQuery($query, $bind_vars));
		$request->setTabletType($tablet_type);
		$request->setNotInTransaction($not_in_transaction);
		$request->setKeyspace($keyspace);
		$request->setKeyspaceIds($keyspace_ids);
		if ($ctx->getCallerId()) {
			$request->setCallerId($ctx->getCallerId());
		}
		$response = $this->client->executeKeyspaceIds($ctx, $request);
		$this->session = $response->getSession();
		VTProto::checkError($response);
		return new VTCursor($response->getResult());
	}

	public function executeKeyRanges(VTContext $ctx, $query, $keyspace, array $key_ranges, array $bind_vars, $tablet_type = \topodata\TabletType::MASTER, $not_in_transaction = FALSE) {
		if (! $this->inTransaction()) {
			throw new VTException('execute called while not in transaction.');
		}
		
		$request = new \vtgate\ExecuteKeyRangesRequest();
		$request->setSession($this->session);
		$request->setQuery(VTProto::BoundQuery($query, $bind_vars));
		$request->setTabletType($tablet_type);
		$request->setNotInTransaction($not_in_transaction);
		$request->setKeyspace($keyspace);
		VTProto::addKeyRanges($request, $key_ranges);
		if ($ctx->getCallerId()) {
			$request->setCallerId($ctx->getCallerId());
		}
		$response = $this->client->executeKeyRanges($ctx, $request);
		$this->session = $response->getSession();
		VTProto::checkError($response);
		return new VTCursor($response->getResult());
	}

	public function executeEntityIds(VTContext $ctx, $query, $keyspace, $entity_column_name, array $entity_keyspace_ids, array $bind_vars, $tablet_type = \topodata\TabletType::MASTER, $not_in_transaction = FALSE) {
		if (! $this->inTransaction()) {
			throw new VTException('execute called while not in transaction.');
		}
		
		$request = new \vtgate\ExecuteEntityIdsRequest();
		$request->setSession($this->session);
		$request->setQuery(VTProto::BoundQuery($query, $bind_vars));
		$request->setTabletType($tablet_type);
		$request->setNotInTransaction($not_in_transaction);
		$request->setKeyspace($keyspace);
		$request->setEntityColumnName($entity_column_name);
		VTProto::addEntityKeyspaceIds($request, $entity_keyspace_ids);
		if ($ctx->getCallerId()) {
			$request->setCallerId($ctx->getCallerId());
		}
		$response = $this->client->executeEntityIds($ctx, $request);
		$this->session = $response->getSession();
		VTProto::checkError($response);
		return new VTCursor($response->getResult());
	}

	public function executeBatchShards(VTContext $ctx, array $bound_shard_queries, $tablet_type = \topodata\TabletType::MASTER, $as_transaction = TRUE) {
		if (! $this->inTransaction()) {
			throw new VTException('execute called while not in transaction.');
		}
		
		$request = new \vtgate\ExecuteBatchShardsRequest();
		$request->setSession($this->session);
		VTProto::addQueries($request, $bound_shard_queries);
		$request->setTabletType($tablet_type);
		$request->setAsTransaction($as_transaction);
		if ($ctx->getCallerId()) {
			$request->setCallerId($ctx->getCallerId());
		}
		$response = $this->client->executeBatchShards($ctx, $request);
		$this->session = $response->getSession();
		VTProto::checkError($response);
		$results = array();
		foreach ($response->getResultsList() as $result) {
			$results[] = new VTCursor($result);
		}
		return $results;
	}

	public function executeBatchKeyspaceIds(VTContext $ctx, array $bound_keyspace_id_queries, $tablet_type = \topodata\TabletType::MASTER, $as_transaction = TRUE) {
		if (! $this->inTransaction()) {
			throw new VTException('execute called while not in transaction.');
		}
		
		$request = new \vtgate\ExecuteBatchKeyspaceIdsRequest();
		$request->setSession($this->session);
		VTProto::addQueries($request, $bound_keyspace_id_queries);
		$request->setTabletType($tablet_type);
		$request->setAsTransaction($as_transaction);
		if ($ctx->getCallerId()) {
			$request->setCallerId($ctx->getCallerId());
		}
		$response = $this->client->executeBatchKeyspaceIds($ctx, $request);
		$this->session = $response->getSession();
		VTProto::checkError($response);
		$results = array();
		foreach ($response->getResultsList() as $result) {
			$results[] = new VTCursor($result);
		}
		return $results;
	}

	public function commit(VTContext $ctx) {
		if (! $this->inTransaction()) {
			throw new VTException('commit called while not in transaction.');
		}
		$request = new \vtgate\CommitRequest();
		$request->setSession($this->session);
		if ($ctx->getCallerId()) {
			$request->setCallerId($ctx->getCallerId());
		}
		
		$response = $this->client->commit($ctx, $request);
		$this->session = NULL;
	}

	public function rollback(VTContext $ctx) {
		if (! $this->inTransaction()) {
			throw new VTException('rollback called while not in transaction.');
		}
		$request = new \vtgate\RollbackRequest();
		$request->setSession($this->session);
		if ($ctx->getCallerId()) {
			$request->setCallerId($ctx->getCallerId());
		}
		
		$response = $this->client->rollback($ctx, $request);
		$this->session = NULL;
	}
}
