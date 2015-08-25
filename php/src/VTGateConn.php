<?php
require_once (dirname(__FILE__) . '/VTProto.php');
require_once (dirname(__FILE__) . '/VTContext.php');

class VTGateTx {
	protected $client;
	protected $session;

	public function __construct($client, $session) {
		$this->client = $client;
		$this->session = $session;
	}

	private function callExecute(VTContext $ctx, $query, array $bind_vars, $tablet_type, $not_in_transaction, $method, $req = array()) {
		if (is_null($this->session)) {
			throw new Exception('execute called while not in transaction.');
		}
		$req['Session'] = $this->session;
		$req['Query'] = VTBoundQuery::buildBsonP3($query, $bind_vars);
		$req['TabletType'] = $tablet_type;
		$req['NotInTransaction'] = $not_in_transaction;
		if ($ctx->getCallerId()) {
			$req['CallerId'] = $ctx->getCallerId()->toBsonP3();
		}
		
		$resp = $this->client->call($ctx, $method, $req)->reply;
		if (array_key_exists('Session', $resp) && $resp['Session']) {
			$this->session = $resp['Session'];
		} else {
			$this->session = NULL;
		}
		VTProto::checkError($resp);
		
		return VTQueryResult::fromBsonP3($resp['Result']);
	}

	private function callExecuteBatch(VTContext $ctx, $queries, $tablet_type, $as_transaction, $method, $req = array()) {
		if (is_null($this->session)) {
			throw new Exception('execute called while not in transaction.');
		}
		$req['Session'] = $this->session;
		$req['Queries'] = $queries;
		$req['TabletType'] = $tablet_type;
		$req['AsTransaction'] = $as_transaction;
		if ($ctx->getCallerId()) {
			$req['CallerId'] = $ctx->getCallerId()->toBsonP3();
		}
		
		$resp = $this->client->call($ctx, $method, $req)->reply;
		if (array_key_exists('Session', $resp) && $resp['Session']) {
			$this->session = $resp['Session'];
		} else {
			$this->session = NULL;
		}
		VTProto::checkError($resp);
		
		$results = array();
		foreach ($resp['Results'] as $result) {
			$results[] = VTQueryResult::fromBsonP3($result);
		}
		return $results;
	}

	public function execute(VTContext $ctx, $query, array $bind_vars, $tablet_type = VTTabletType::MASTER, $not_in_transaction = FALSE) {
		return $this->callExecute($ctx, $query, $bind_vars, $tablet_type, $not_in_transaction, 'VTGateP3.Execute');
	}

	public function executeShards(VTContext $ctx, $query, $keyspace, array $shards, array $bind_vars, $tablet_type = VTTabletType::MASTER, $not_in_transaction = FALSE) {
		return $this->callExecute($ctx, $query, $bind_vars, $tablet_type, $not_in_transaction, 'VTGateP3.ExecuteShards', array(
				'Keyspace' => $keyspace,
				'Shards' => $shards 
		));
	}

	public function executeKeyspaceIds(VTContext $ctx, $query, $keyspace, array $keyspace_ids, array $bind_vars, $tablet_type = VTTabletType::MASTER, $not_in_transaction = FALSE) {
		return $this->callExecute($ctx, $query, $bind_vars, $tablet_type, $not_in_transaction, 'VTGateP3.ExecuteKeyspaceIds', array(
				'Keyspace' => $keyspace,
				'KeyspaceIds' => VTKeyspaceId::buildBsonP3Array($keyspace_ids) 
		));
	}

	public function executeKeyRanges(VTContext $ctx, $query, $keyspace, array $key_ranges, array $bind_vars, $tablet_type = VTTabletType::MASTER, $not_in_transaction = FALSE) {
		return $this->callExecute($ctx, $query, $bind_vars, $tablet_type, $not_in_transaction, 'VTGateP3.ExecuteKeyRanges', array(
				'Keyspace' => $keyspace,
				'KeyRanges' => VTKeyRange::buildBsonP3Array($key_ranges) 
		));
	}

	public function executeEntityIds(VTContext $ctx, $query, $keyspace, $entity_column_name, array $entity_keyspace_ids, array $bind_vars, $tablet_type = VTTabletType::MASTER, $not_in_transaction = FALSE) {
		return $this->callExecute($ctx, $query, $bind_vars, $tablet_type, $not_in_transaction, 'VTGateP3.ExecuteEntityIds', array(
				'Keyspace' => $keyspace,
				'EntityColumnName' => $entity_column_name,
				'EntityKeyspaceIds' => VTEntityId::buildBsonP3Array($entity_keyspace_ids) 
		));
	}

	public function executeBatchShards(VTContext $ctx, array $bound_shard_queries, $tablet_type = VTTabletType::MASTER, $as_transaction = TRUE) {
		return $this->callExecuteBatch($ctx, VTBoundShardQuery::buildBsonP3Array($bound_shard_queries), $tablet_type, $as_transaction, 'VTGateP3.ExecuteBatchShards');
	}

	public function executeBatchKeyspaceIds(VTContext $ctx, array $bound_keyspace_id_queries, $tablet_type = VTTabletType::MASTER, $as_transaction = TRUE) {
		return $this->callExecuteBatch($ctx, VTBoundKeyspaceIdQuery::buildBsonP3Array($bound_keyspace_id_queries), $tablet_type, $as_transaction, 'VTGateP3.ExecuteBatchKeyspaceIds');
	}

	public function commit(VTContext $ctx) {
		if (is_null($this->session)) {
			throw new Exception('commit called while not in transaction.');
		}
		$req = array(
				'Session' => $this->session 
		);
		if ($ctx->getCallerId()) {
			$req['CallerId'] = $ctx->getCallerId()->toBsonP3();
		}
		
		$resp = $this->client->call($ctx, 'VTGateP3.Commit2', $req)->reply;
		$this->session = NULL;
	}

	public function rollback(VTContext $ctx) {
		if (is_null($this->session)) {
			throw new Exception('rollback called while not in transaction.');
		}
		$req = array(
				'Session' => $this->session 
		);
		if ($ctx->getCallerId()) {
			$req['CallerId'] = $ctx->getCallerId()->toBsonP3();
		}
		
		$resp = $this->client->call($ctx, 'VTGateP3.Rollback2', $req)->reply;
		$this->session = NULL;
	}
}

class VTGateConn {
	protected $client;

	public function __construct($client) {
		$this->client = $client;
	}

	private function callExecute(VTContext $ctx, $query, array $bind_vars, $tablet_type, $method, $req = array()) {
		$req['Query'] = VTBoundQuery::buildBsonP3($query, $bind_vars);
		$req['TabletType'] = $tablet_type;
		if ($ctx->getCallerId()) {
			$req['CallerId'] = $ctx->getCallerId()->toBsonP3();
		}
		
		$resp = $this->client->call($ctx, $method, $req)->reply;
		VTProto::checkError($resp);
		
		return VTQueryResult::fromBsonP3($resp['Result']);
	}

	private function callExecuteBatch(VTContext $ctx, $queries, $tablet_type, $as_transaction, $method, $req = array()) {
		$req['Queries'] = $queries;
		$req['TabletType'] = $tablet_type;
		$req['AsTransaction'] = $as_transaction;
		if ($ctx->getCallerId()) {
			$req['CallerId'] = $ctx->getCallerId()->toBsonP3();
		}
		
		$resp = $this->client->call($ctx, $method, $req)->reply;
		VTProto::checkError($resp);
		
		$results = array();
		if (array_key_exists('Results', $resp)) {
			foreach ($resp['Results'] as $result) {
				$results[] = VTQueryResult::fromBsonP3($result);
			}
		}
		return $results;
	}

	public function execute(VTContext $ctx, $query, array $bind_vars, $tablet_type) {
		return $this->callExecute($ctx, $query, $bind_vars, $tablet_type, 'VTGateP3.Execute');
	}

	public function executeShards(VTContext $ctx, $query, $keyspace, array $shards, array $bind_vars, $tablet_type) {
		return $this->callExecute($ctx, $query, $bind_vars, $tablet_type, 'VTGateP3.ExecuteShards', array(
				'Keyspace' => $keyspace,
				'Shards' => $shards 
		));
	}

	public function executeKeyspaceIds(VTContext $ctx, $query, $keyspace, array $keyspace_ids, array $bind_vars, $tablet_type) {
		return $this->callExecute($ctx, $query, $bind_vars, $tablet_type, 'VTGateP3.ExecuteKeyspaceIds', array(
				'Keyspace' => $keyspace,
				'KeyspaceIds' => VTKeyspaceId::buildBsonP3Array($keyspace_ids) 
		));
	}

	public function executeKeyRanges(VTContext $ctx, $query, $keyspace, array $key_ranges, array $bind_vars, $tablet_type) {
		return $this->callExecute($ctx, $query, $bind_vars, $tablet_type, 'VTGateP3.ExecuteKeyRanges', array(
				'Keyspace' => $keyspace,
				'KeyRanges' => VTKeyRange::buildBsonP3Array($key_ranges) 
		));
	}

	public function executeEntityIds(VTContext $ctx, $query, $keyspace, $entity_column_name, array $entity_keyspace_ids, array $bind_vars, $tablet_type) {
		return $this->callExecute($ctx, $query, $bind_vars, $tablet_type, 'VTGateP3.ExecuteEntityIds', array(
				'Keyspace' => $keyspace,
				'EntityColumnName' => $entity_column_name,
				'EntityKeyspaceIds' => VTEntityId::buildBsonP3Array($entity_keyspace_ids) 
		));
	}

	public function executeBatchShards(VTContext $ctx, array $bound_shard_queries, $tablet_type, $as_transaction) {
		return $this->callExecuteBatch($ctx, VTBoundShardQuery::buildBsonP3Array($bound_shard_queries), $tablet_type, $as_transaction, 'VTGateP3.ExecuteBatchShards');
	}

	public function executeBatchKeyspaceIds(VTContext $ctx, array $bound_keyspace_id_queries, $tablet_type, $as_transaction) {
		return $this->callExecuteBatch($ctx, VTBoundKeyspaceIdQuery::buildBsonP3Array($bound_keyspace_id_queries), $tablet_type, $as_transaction, 'VTGateP3.ExecuteBatchKeyspaceIds');
	}

	public function begin(VTContext $ctx) {
		$req = array();
		if ($ctx->getCallerId()) {
			$req['CallerId'] = $ctx->getCallerId()->toBsonP3();
		}
		
		$resp = $this->client->call($ctx, 'VTGateP3.Begin2', $req)->reply;
		
		return new VTGateTx($this->client, $resp['Session']);
	}

	public function splitQuery(VTContext $ctx, $keyspace, $query, array $bind_vars, $split_column, $split_count) {
		$req = array(
				'Keyspace' => $keyspace,
				'Query' => VTBoundQuery::buildBsonP3($query, $bind_vars),
				'SplitColumn' => $split_column,
				'SplitCount' => $split_count 
		);
		if ($ctx->getCallerId()) {
			$req['CallerId'] = $ctx->getCallerId()->toBsonP3();
		}
		
		$resp = $this->client->call($ctx, 'VTGateP3.SplitQuery', $req)->reply;
		
		$results = array();
		if (array_key_exists('Splits', $resp)) {
			foreach ($resp['Splits'] as $split) {
				$results[] = VTSplitQueryPart::fromBsonP3($split);
			}
		}
		return $results;
	}

	public function getSrvKeyspace(VTContext $ctx, $keyspace) {
		$req = array(
				'Keyspace' => $keyspace 
		);
		if ($ctx->getCallerId()) {
			$req['CallerId'] = $ctx->getCallerId()->toBsonP3();
		}
		
		$resp = $this->client->call($ctx, 'VTGateP3.GetSrvKeyspace', $req)->reply;
		return VTSrvKeyspace::fromBsonP3($resp['SrvKeyspace']);
	}

	public function close() {
		$this->client->close();
	}
}
