<?php
namespace Vitess\Grpc;

use \Vitess\Context;
use \Vitess\Proto;

class Client implements \Vitess\RpcClient
{

    protected $stub;

    public function __construct($addr, $opts = [])
    {
        $this->stub = new Proto\Vtgateservice\VitessClient($addr, $opts);
    }

    public function execute(Context $ctx, Proto\Vtgate\ExecuteRequest $request)
    {
        list ($response, $status) = $this->stub->Execute($request)->wait();
        self::checkError($status);
        return $response;
    }

    public function executeShards(Context $ctx, Proto\Vtgate\ExecuteShardsRequest $request)
    {
        list ($response, $status) = $this->stub->ExecuteShards($request)->wait();
        self::checkError($status);
        return $response;
    }

    public function executeKeyspaceIds(Context $ctx, Proto\Vtgate\ExecuteKeyspaceIdsRequest $request)
    {
        list ($response, $status) = $this->stub->ExecuteKeyspaceIds($request)->wait();
        self::checkError($status);
        return $response;
    }

    public function executeKeyRanges(Context $ctx, Proto\Vtgate\ExecuteKeyRangesRequest $request)
    {
        list ($response, $status) = $this->stub->ExecuteKeyRanges($request)->wait();
        self::checkError($status);
        return $response;
    }

    public function executeEntityIds(Context $ctx, Proto\Vtgate\ExecuteEntityIdsRequest $request)
    {
        list ($response, $status) = $this->stub->ExecuteEntityIds($request)->wait();
        self::checkError($status);
        return $response;
    }

    public function executeBatchShards(Context $ctx, Proto\Vtgate\ExecuteBatchShardsRequest $request)
    {
        list ($response, $status) = $this->stub->ExecuteBatchShards($request)->wait();
        self::checkError($status);
        return $response;
    }

    public function executeBatchKeyspaceIds(Context $ctx, Proto\Vtgate\ExecuteBatchKeyspaceIdsRequest $request)
    {
        list ($response, $status) = $this->stub->ExecuteBatchKeyspaceIds($request)->wait();
        self::checkError($status);
        return $response;
    }

    public function streamExecute(Context $ctx, Proto\Vtgate\StreamExecuteRequest $request)
    {
        return new StreamResponse($this->stub->StreamExecute($request));
    }

    public function streamExecuteShards(Context $ctx, Proto\Vtgate\StreamExecuteShardsRequest $request)
    {
        return new StreamResponse($this->stub->StreamExecuteShards($request));
    }

    public function streamExecuteKeyspaceIds(Context $ctx, Proto\Vtgate\StreamExecuteKeyspaceIdsRequest $request)
    {
        return new StreamResponse($this->stub->StreamExecuteKeyspaceIds($request));
    }

    public function streamExecuteKeyRanges(Context $ctx, Proto\Vtgate\StreamExecuteKeyRangesRequest $request)
    {
        return new StreamResponse($this->stub->StreamExecuteKeyRanges($request));
    }

    public function begin(Context $ctx, Proto\Vtgate\BeginRequest $request)
    {
        list ($response, $status) = $this->stub->Begin($request)->wait();
        self::checkError($status);
        return $response;
    }

    public function commit(Context $ctx, Proto\Vtgate\CommitRequest $request)
    {
        list ($response, $status) = $this->stub->Commit($request)->wait();
        self::checkError($status);
        return $response;
    }

    public function rollback(Context $ctx, Proto\Vtgate\RollbackRequest $request)
    {
        list ($response, $status) = $this->stub->Rollback($request)->wait();
        self::checkError($status);
        return $response;
    }

    public function getSrvKeyspace(Context $ctx, Proto\Vtgate\GetSrvKeyspaceRequest $request)
    {
        list ($response, $status) = $this->stub->GetSrvKeyspace($request)->wait();
        self::checkError($status);
        return $response;
    }

    public function splitQuery(Context $ctx, Proto\Vtgate\SplitQueryRequest $request)
    {
        list ($response, $status) = $this->stub->SplitQuery($request)->wait();
        self::checkError($status);
        return $response;
    }

    public function close()
    {
        $this->stub->close();
    }

    public static function checkError($status)
    {
        if ($status) {
            switch ($status->code) {
                case \Grpc\STATUS_OK:
                    break;
                case \Grpc\STATUS_INVALID_ARGUMENT:
                    throw new \Vitess\Error\BadInput($status->details);
                case \Grpc\STATUS_DEADLINE_EXCEEDED:
                    throw new \Vitess\Error\DeadlineExceeded($status->details);
                case \Grpc\STATUS_ALREADY_EXISTS:
                    throw new \Vitess\Error\Integrity($status->details);
                case \Grpc\STATUS_UNAUTHENTICATED:
                    throw new \Vitess\Error\Unauthenticated($status->details);
                case \Grpc\STATUS_UNAVAILABLE:
                    throw new \Vitess\Error\Transient($status->details);
                case \Grpc\STATUS_ABORTED:
                    throw new \Vitess\Error\Aborted($status->details);
                default:
                    throw new \Vitess\Exception("{$status->code}: {$status->details}");
            }
        }
    }
}
