<?php
namespace Vitess;

use Vitess\Proto\Vtgate\Session;

class VTGateConn
{
    /**
     *
     * @var RpcClient The underlying RPC client.
     */
    protected $client;

    /**
     * @var Session
     */
    protected $session;

    /**
     * Create a VTGateConn.
     *
     * If specified, the given keyspace will be used as the connection-wide
     * default for execute() and streamExecute() calls, since those do not
     * specify the keyspace for each call. Like the connection-wide default
     * database of a MySQL connection, individual queries can still refer to
     * other keyspaces by prefixing table names. For example:
     * "SELECT ... FROM keyspace.table ..."
     *
     * If the default keyspace name is left empty, Vitess will use VSchema to
     * look up the keyspace for each unprefixed table name. Note that this only
     * works if the table name is unique across all keyspaces.
     *
     * @param RpcClient $client
     * @param string $keyspace
     *            The connection-wide keyspace for execute() calls.
     */
    public function __construct(RpcClient $client, $keyspace = '')
    {
        $this->client = $client;

        $session = new Proto\Vtgate\Session();
        $session->setTargetString($keyspace);
        $session->setAutocommit(true);
        $this->session = $session;
    }

    public function execute(Context $ctx, $query, array $bind_vars)
    {
        $request = new Proto\Vtgate\ExecuteRequest();
        $request->setSession($this->session);
        $request->setQuery(ProtoUtils::BoundQuery($query, $bind_vars));
        if ($ctx->getCallerId()) {
            $request->setCallerId($ctx->getCallerId());
        }
        $response = $this->client->execute($ctx, $request);
        $this->session = $response->getSession();
        ProtoUtils::checkError($response);
        return new Cursor($response->getResult());
    }

    public function streamExecute(Context $ctx, $query, array $bind_vars)
    {
        $request = new Proto\Vtgate\StreamExecuteRequest();
        $request->setSession($this->session);
        $request->setQuery(ProtoUtils::BoundQuery($query, $bind_vars));
        if ($ctx->getCallerId()) {
            $request->setCallerId($ctx->getCallerId());
        }
        $call = $this->client->streamExecute($ctx, $request);
        return new StreamCursor($call);
    }

    public function begin(Context $ctx)
    {
        $this->execute($ctx, 'BEGIN', array());
        
        return $this;
    }

    public function commit(Context $ctx)
    {
        $this->execute($ctx, 'COMMIT', array());

        return $this;
    }

    public function rollback(Context $ctx)
    {
        $this->execute($ctx, 'ROLLBACK', array());

        return $this;
    }
}
