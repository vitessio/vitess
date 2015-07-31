package com.youtube.vitess.client;

import com.youtube.vitess.proto.Vtgate.BeginRequest;
import com.youtube.vitess.proto.Vtgate.BeginResponse;
import com.youtube.vitess.proto.Vtgate.CommitRequest;
import com.youtube.vitess.proto.Vtgate.CommitResponse;
import com.youtube.vitess.proto.Vtgate.ExecuteBatchKeyspaceIdsRequest;
import com.youtube.vitess.proto.Vtgate.ExecuteBatchKeyspaceIdsResponse;
import com.youtube.vitess.proto.Vtgate.ExecuteBatchShardsRequest;
import com.youtube.vitess.proto.Vtgate.ExecuteBatchShardsResponse;
import com.youtube.vitess.proto.Vtgate.ExecuteKeyRangesRequest;
import com.youtube.vitess.proto.Vtgate.ExecuteKeyRangesResponse;
import com.youtube.vitess.proto.Vtgate.ExecuteKeyspaceIdsRequest;
import com.youtube.vitess.proto.Vtgate.ExecuteKeyspaceIdsResponse;
import com.youtube.vitess.proto.Vtgate.ExecuteShardsRequest;
import com.youtube.vitess.proto.Vtgate.ExecuteShardsResponse;
import com.youtube.vitess.proto.Vtgate.GetSrvKeyspaceRequest;
import com.youtube.vitess.proto.Vtgate.GetSrvKeyspaceResponse;
import com.youtube.vitess.proto.Vtgate.RollbackRequest;
import com.youtube.vitess.proto.Vtgate.RollbackResponse;
import com.youtube.vitess.proto.Vtgate.StreamExecuteKeyRangesRequest;
import com.youtube.vitess.proto.Vtgate.StreamExecuteKeyRangesResponse;
import com.youtube.vitess.proto.Vtgate.StreamExecuteKeyspaceIdsRequest;
import com.youtube.vitess.proto.Vtgate.StreamExecuteKeyspaceIdsResponse;
import com.youtube.vitess.proto.Vtgate.StreamExecuteShardsRequest;
import com.youtube.vitess.proto.Vtgate.StreamExecuteShardsResponse;

import java.io.Closeable;
import java.util.Iterator;

/**
 * RpcClient defines a set of methods to communicate with VTGates.
 */
public interface RpcClient extends Closeable {
  // executeShard sends a single query to a set of shards.
  ExecuteShardsResponse executeShard(
      Context ctx, ExecuteShardsRequest request) throws VitessRpcException;

  // executeKeyspaceIds sends a query with keyspace ids as bind variables.
  ExecuteKeyspaceIdsResponse executeKeyspaceIds(
      Context ctx, ExecuteKeyspaceIdsRequest request) throws VitessRpcException;

  // executeKeyRanges sends a query with a set of key ranges.
  ExecuteKeyRangesResponse executeKeyRanges(
      Context ctx, ExecuteKeyRangesRequest request) throws VitessRpcException;

  // executeBatchKeyspaceIds sends a list of queries with keyspace ids as bind variables.
  ExecuteBatchKeyspaceIdsResponse executeBatchKeyspaceIds(
      Context ctx, ExecuteBatchKeyspaceIdsRequest request) throws VitessRpcException;

  // executeBatchShards sends a list of queries to a set of shards.
  ExecuteBatchShardsResponse executeBatchShards(
      Context ctx, ExecuteBatchShardsRequest request) throws VitessRpcException;

  // streamExecuteShard starts stream queries with multiple shards.
  Iterator<StreamExecuteShardsResponse> streamExecuteShard(
      Context ctx, StreamExecuteShardsRequest request) throws VitessRpcException;

  // streamExecuteKeyspaceIds starts a list of stream queries with keyspace ids as bind variables.
  Iterator<StreamExecuteKeyspaceIdsResponse> streamExecuteKeyspaceIds(
      Context ctx, StreamExecuteKeyspaceIdsRequest request) throws VitessRpcException;

  // streamExecuteKeyRanges starts stream query with a set of key ranges.
  Iterator<StreamExecuteKeyRangesResponse> streamExecuteKeyRanges(
      Context ctx, StreamExecuteKeyRangesRequest request) throws VitessRpcException;

  // begin starts a transaction.
  BeginResponse begin(Context ctx, BeginRequest request) throws VitessRpcException;

  // commit commits a transaction.
  CommitResponse commit(Context ctx, CommitRequest request) throws VitessRpcException;

  // rollback rollbacks a pending transaction.
  RollbackResponse rollback(Context ctx, RollbackRequest request) throws VitessRpcException;

  // getSrvKeyspace returns a list of serving keyspaces.
  GetSrvKeyspaceResponse getSrvKeyspace(
      Context ctx, GetSrvKeyspaceRequest request) throws VitessRpcException;
}
