<?php
namespace Vitess;

class VTGateTx
{

    protected $client;

    protected $session;

    protected $keyspace;

    public function __construct($client, $session, $keyspace = '')
    {
        $this->client = $client;
        $this->session = $session;
        $this->keyspace = $keyspace;
    }

    private function inTransaction()
    {
        return ! is_null($this->session) && $this->session->getInTransaction();
    }

    public function execute(Context $ctx, $query, array $bind_vars, $tablet_type = Proto\Topodata\TabletType::MASTER)
    {
        if (! $this->inTransaction()) {
            throw new \Vitess\Exception('execute called while not in transaction.');
        }

        $request = new Proto\Vtgate\ExecuteRequest();
        $request->setSession($this->session);
        $request->setQuery(ProtoUtils::BoundQuery($query, $bind_vars));
        $request->setTabletType($tablet_type);
        $request->setKeyspaceShard($this->keyspace);
        if ($ctx->getCallerId()) {
            $request->setCallerId($ctx->getCallerId());
        }
        $response = $this->client->execute($ctx, $request);
        $this->session = $response->getSession();
        ProtoUtils::checkError($response);
        return new Cursor($response->getResult());
    }

    public function executeShards(Context $ctx, $query, $keyspace, array $shards, array $bind_vars, $tablet_type = Proto\Topodata\TabletType::MASTER)
    {
        if (! $this->inTransaction()) {
            throw new \Vitess\Exception('execute called while not in transaction.');
        }

        $request = new Proto\Vtgate\ExecuteShardsRequest();
        $request->setSession($this->session);
        $request->setQuery(ProtoUtils::BoundQuery($query, $bind_vars));
        $request->setTabletType($tablet_type);
        $request->setKeyspace($keyspace);
        $request->setShards($shards);
        if ($ctx->getCallerId()) {
            $request->setCallerId($ctx->getCallerId());
        }
        $response = $this->client->executeShards($ctx, $request);
        $this->session = $response->getSession();
        ProtoUtils::checkError($response);
        return new Cursor($response->getResult());
    }

    public function executeKeyspaceIds(Context $ctx, $query, $keyspace, array $keyspace_ids, array $bind_vars, $tablet_type = Proto\Topodata\TabletType::MASTER)
    {
        if (! $this->inTransaction()) {
            throw new \Vitess\Exception('execute called while not in transaction.');
        }

        $request = new Proto\Vtgate\ExecuteKeyspaceIdsRequest();
        $request->setSession($this->session);
        $request->setQuery(ProtoUtils::BoundQuery($query, $bind_vars));
        $request->setTabletType($tablet_type);
        $request->setKeyspace($keyspace);
        $request->setKeyspaceIds($keyspace_ids);
        if ($ctx->getCallerId()) {
            $request->setCallerId($ctx->getCallerId());
        }
        $response = $this->client->executeKeyspaceIds($ctx, $request);
        $this->session = $response->getSession();
        ProtoUtils::checkError($response);
        return new Cursor($response->getResult());
    }

    public function executeKeyRanges(Context $ctx, $query, $keyspace, array $key_ranges, array $bind_vars, $tablet_type = Proto\Topodata\TabletType::MASTER)
    {
        if (! $this->inTransaction()) {
            throw new \Vitess\Exception('execute called while not in transaction.');
        }

        $request = new Proto\Vtgate\ExecuteKeyRangesRequest();
        $request->setSession($this->session);
        $request->setQuery(ProtoUtils::BoundQuery($query, $bind_vars));
        $request->setTabletType($tablet_type);
        $request->setKeyspace($keyspace);
        ProtoUtils::addKeyRanges($request, $key_ranges);
        if ($ctx->getCallerId()) {
            $request->setCallerId($ctx->getCallerId());
        }
        $response = $this->client->executeKeyRanges($ctx, $request);
        $this->session = $response->getSession();
        ProtoUtils::checkError($response);
        return new Cursor($response->getResult());
    }

    public function executeEntityIds(Context $ctx, $query, $keyspace, $entity_column_name, array $entity_keyspace_ids, array $bind_vars, $tablet_type = Proto\Topodata\TabletType::MASTER)
    {
        if (! $this->inTransaction()) {
            throw new \Vitess\Exception('execute called while not in transaction.');
        }

        $request = new Proto\Vtgate\ExecuteEntityIdsRequest();
        $request->setSession($this->session);
        $request->setQuery(ProtoUtils::BoundQuery($query, $bind_vars));
        $request->setTabletType($tablet_type);
        $request->setKeyspace($keyspace);
        $request->setEntityColumnName($entity_column_name);
        ProtoUtils::addEntityKeyspaceIds($request, $entity_keyspace_ids);
        if ($ctx->getCallerId()) {
            $request->setCallerId($ctx->getCallerId());
        }
        $response = $this->client->executeEntityIds($ctx, $request);
        $this->session = $response->getSession();
        ProtoUtils::checkError($response);
        return new Cursor($response->getResult());
    }

    public function executeBatchShards(Context $ctx, array $bound_shard_queries, $tablet_type = Proto\Topodata\TabletType::MASTER)
    {
        if (! $this->inTransaction()) {
            throw new \Vitess\Exception('execute called while not in transaction.');
        }

        $request = new Proto\Vtgate\ExecuteBatchShardsRequest();
        $request->setSession($this->session);
        ProtoUtils::addQueries($request, $bound_shard_queries);
        $request->setTabletType($tablet_type);
        if ($ctx->getCallerId()) {
            $request->setCallerId($ctx->getCallerId());
        }
        $response = $this->client->executeBatchShards($ctx, $request);
        $this->session = $response->getSession();
        ProtoUtils::checkError($response);
        $results = array();
        foreach ($response->getResultsList() as $result) {
            $results[] = new Cursor($result);
        }
        return $results;
    }

    public function executeBatchKeyspaceIds(Context $ctx, array $bound_keyspace_id_queries, $tablet_type = Proto\Topodata\TabletType::MASTER)
    {
        if (! $this->inTransaction()) {
            throw new \Vitess\Exception('execute called while not in transaction.');
        }

        $request = new Proto\Vtgate\ExecuteBatchKeyspaceIdsRequest();
        $request->setSession($this->session);
        ProtoUtils::addQueries($request, $bound_keyspace_id_queries);
        $request->setTabletType($tablet_type);
        if ($ctx->getCallerId()) {
            $request->setCallerId($ctx->getCallerId());
        }
        $response = $this->client->executeBatchKeyspaceIds($ctx, $request);
        $this->session = $response->getSession();
        ProtoUtils::checkError($response);
        $results = array();
        foreach ($response->getResultsList() as $result) {
            $results[] = new Cursor($result);
        }
        return $results;
    }

    public function commit(Context $ctx)
    {
        if (! $this->inTransaction()) {
            throw new \Vitess\Exception('commit called while not in transaction.');
        }
        $request = new Proto\Vtgate\CommitRequest();
        $request->setSession($this->session);
        if ($ctx->getCallerId()) {
            $request->setCallerId($ctx->getCallerId());
        }

        $response = $this->client->commit($ctx, $request);
        $this->session = NULL;
    }

    public function rollback(Context $ctx)
    {
        if (! $this->inTransaction()) {
            throw new \Vitess\Exception('rollback called while not in transaction.');
        }
        $request = new Proto\Vtgate\RollbackRequest();
        $request->setSession($this->session);
        if ($ctx->getCallerId()) {
            $request->setCallerId($ctx->getCallerId());
        }

        $response = $this->client->rollback($ctx, $request);
        $this->session = NULL;
    }
}
