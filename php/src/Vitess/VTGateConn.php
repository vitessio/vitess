<?php
namespace Vitess;

class VTGateConn
{

    /**
     *
     * @var RpcClient
     */
    protected $client;

    public function __construct(RpcClient $client)
    {
        $this->client = $client;
    }

    public function execute(Context $ctx, $query, array $bind_vars, $tablet_type)
    {
        $request = new Proto\Vtgate\ExecuteRequest();
        $request->setQuery(ProtoUtils::BoundQuery($query, $bind_vars));
        $request->setTabletType($tablet_type);
        if ($ctx->getCallerId()) {
            $request->setCallerId($ctx->getCallerId());
        }
        $response = $this->client->execute($ctx, $request);
        ProtoUtils::checkError($response);
        return new Cursor($response->getResult());
    }

    public function executeShards(Context $ctx, $query, $keyspace, array $shards, array $bind_vars, $tablet_type)
    {
        $request = new Proto\Vtgate\ExecuteShardsRequest();
        $request->setQuery(ProtoUtils::BoundQuery($query, $bind_vars));
        $request->setTabletType($tablet_type);
        $request->setKeyspace($keyspace);
        $request->setShards($shards);
        if ($ctx->getCallerId()) {
            $request->setCallerId($ctx->getCallerId());
        }
        $response = $this->client->executeShards($ctx, $request);
        ProtoUtils::checkError($response);
        return new Cursor($response->getResult());
    }

    public function executeKeyspaceIds(Context $ctx, $query, $keyspace, array $keyspace_ids, array $bind_vars, $tablet_type)
    {
        $request = new Proto\Vtgate\ExecuteKeyspaceIdsRequest();
        $request->setQuery(ProtoUtils::BoundQuery($query, $bind_vars));
        $request->setTabletType($tablet_type);
        $request->setKeyspace($keyspace);
        $request->setKeyspaceIds($keyspace_ids);
        if ($ctx->getCallerId()) {
            $request->setCallerId($ctx->getCallerId());
        }
        $response = $this->client->executeKeyspaceIds($ctx, $request);
        ProtoUtils::checkError($response);
        return new Cursor($response->getResult());
    }

    public function executeKeyRanges(Context $ctx, $query, $keyspace, array $key_ranges, array $bind_vars, $tablet_type)
    {
        $request = new Proto\Vtgate\ExecuteKeyRangesRequest();
        $request->setQuery(ProtoUtils::BoundQuery($query, $bind_vars));
        $request->setTabletType($tablet_type);
        $request->setKeyspace($keyspace);
        ProtoUtils::addKeyRanges($request, $key_ranges);
        if ($ctx->getCallerId()) {
            $request->setCallerId($ctx->getCallerId());
        }
        $response = $this->client->executeKeyRanges($ctx, $request);
        ProtoUtils::checkError($response);
        return new Cursor($response->getResult());
    }

    public function executeEntityIds(Context $ctx, $query, $keyspace, $entity_column_name, array $entity_keyspace_ids, array $bind_vars, $tablet_type)
    {
        $request = new Proto\Vtgate\ExecuteEntityIdsRequest();
        $request->setQuery(ProtoUtils::BoundQuery($query, $bind_vars));
        $request->setTabletType($tablet_type);
        $request->setKeyspace($keyspace);
        $request->setEntityColumnName($entity_column_name);
        ProtoUtils::addEntityKeyspaceIds($request, $entity_keyspace_ids);
        if ($ctx->getCallerId()) {
            $request->setCallerId($ctx->getCallerId());
        }
        $response = $this->client->executeEntityIds($ctx, $request);
        ProtoUtils::checkError($response);
        return new Cursor($response->getResult());
    }

    /**
     * Execute multiple shard queries as a batch.
     *
     * @param boolean $as_transaction
     *            If true, automatically create a transaction (per shard) that
     *            encloses all the batch queries.
     */
    public function executeBatchShards(Context $ctx, array $bound_shard_queries, $tablet_type, $as_transaction)
    {
        $request = new Proto\Vtgate\ExecuteBatchShardsRequest();
        ProtoUtils::addQueries($request, $bound_shard_queries);
        $request->setTabletType($tablet_type);
        $request->setAsTransaction($as_transaction);
        if ($ctx->getCallerId()) {
            $request->setCallerId($ctx->getCallerId());
        }
        $response = $this->client->executeBatchShards($ctx, $request);
        ProtoUtils::checkError($response);
        $results = array();
        foreach ($response->getResultsList() as $result) {
            $results[] = new Cursor($result);
        }
        return $results;
    }

    /**
     * Execute multiple keyspace ID queries as a batch.
     *
     * @param boolean $as_transaction
     *            If true, automatically create a transaction (per shard) that
     *            encloses all the batch queries.
     */
    public function executeBatchKeyspaceIds(Context $ctx, array $bound_keyspace_id_queries, $tablet_type, $as_transaction)
    {
        $request = new Proto\Vtgate\ExecuteBatchKeyspaceIdsRequest();
        ProtoUtils::addQueries($request, $bound_keyspace_id_queries);
        $request->setTabletType($tablet_type);
        $request->setAsTransaction($as_transaction);
        if ($ctx->getCallerId()) {
            $request->setCallerId($ctx->getCallerId());
        }
        $response = $this->client->executeBatchKeyspaceIds($ctx, $request);
        ProtoUtils::checkError($response);
        $results = array();
        foreach ($response->getResultsList() as $result) {
            $results[] = new Cursor($result);
        }
        return $results;
    }

    public function streamExecute(Context $ctx, $query, array $bind_vars, $tablet_type)
    {
        $request = new Proto\Vtgate\StreamExecuteRequest();
        $request->setQuery(ProtoUtils::BoundQuery($query, $bind_vars));
        $request->setTabletType($tablet_type);
        if ($ctx->getCallerId()) {
            $request->setCallerId($ctx->getCallerId());
        }
        $call = $this->client->streamExecute($ctx, $request);
        return new StreamCursor($call);
    }

    public function streamExecuteShards(Context $ctx, $query, $keyspace, array $shards, array $bind_vars, $tablet_type)
    {
        $request = new Proto\Vtgate\StreamExecuteShardsRequest();
        $request->setQuery(ProtoUtils::BoundQuery($query, $bind_vars));
        $request->setKeyspace($keyspace);
        $request->setShards($shards);
        $request->setTabletType($tablet_type);
        if ($ctx->getCallerId()) {
            $request->setCallerId($ctx->getCallerId());
        }
        $call = $this->client->streamExecuteShards($ctx, $request);
        return new StreamCursor($call);
    }

    public function streamExecuteKeyspaceIds(Context $ctx, $query, $keyspace, array $keyspace_ids, array $bind_vars, $tablet_type)
    {
        $request = new Proto\Vtgate\StreamExecuteKeyspaceIdsRequest();
        $request->setQuery(ProtoUtils::BoundQuery($query, $bind_vars));
        $request->setKeyspace($keyspace);
        $request->setKeyspaceIds($keyspace_ids);
        $request->setTabletType($tablet_type);
        if ($ctx->getCallerId()) {
            $request->setCallerId($ctx->getCallerId());
        }
        $call = $this->client->streamExecuteKeyspaceIds($ctx, $request);
        return new StreamCursor($call);
    }

    public function streamExecuteKeyRanges(Context $ctx, $query, $keyspace, array $key_ranges, array $bind_vars, $tablet_type)
    {
        $request = new Proto\Vtgate\StreamExecuteKeyRangesRequest();
        $request->setQuery(ProtoUtils::BoundQuery($query, $bind_vars));
        $request->setKeyspace($keyspace);
        ProtoUtils::addKeyRanges($request, $key_ranges);
        $request->setTabletType($tablet_type);
        if ($ctx->getCallerId()) {
            $request->setCallerId($ctx->getCallerId());
        }
        $call = $this->client->streamExecuteKeyRanges($ctx, $request);
        return new StreamCursor($call);
    }

    public function begin(Context $ctx)
    {
        $request = new Proto\Vtgate\BeginRequest();
        if ($ctx->getCallerId()) {
            $request->setCallerId($ctx->getCallerId());
        }

        $response = $this->client->begin($ctx, $request);
        return new VTGateTx($this->client, $response->getSession());
    }

    // TODO(erez): Migrate to SplitQueryV2 after it's stable.
    public function splitQuery(Context $ctx, $keyspace, $query, array $bind_vars, $split_column, $split_count)
    {
        $request = new Proto\Vtgate\SplitQueryRequest();
        $request->setKeyspace($keyspace);
        $request->setQuery(ProtoUtils::BoundQuery($query, $bind_vars));
        $request->addSplitColumn($split_column);
        $request->setSplitCount($split_count);
        $request->setUseSplitQueryV2(false);
        if ($ctx->getCallerId()) {
            $request->setCallerId($ctx->getCallerId());
        }

        $response = $this->client->splitQuery($ctx, $request);
        return $response->getSplitsList();
    }

    public function getSrvKeyspace(Context $ctx, $keyspace)
    {
        $request = new Proto\Vtgate\GetSrvKeyspaceRequest();
        $request->setKeyspace($keyspace);
        $response = $this->client->getSrvKeyspace($ctx, $request);
        return $response->getSrvKeyspace();
    }

    public function close()
    {
        $this->client->close();
    }
}
