package com.youtube.vitess.vtgate.rpcclient.gorpc;

import java.util.List;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.bson.BSONObject;
import org.bson.BasicBSONObject;

import com.youtube.vitess.gorpc.Client;
import com.youtube.vitess.gorpc.Exceptions.ApplicationException;
import com.youtube.vitess.gorpc.Exceptions.GoRpcException;
import com.youtube.vitess.gorpc.Response;
import com.youtube.vitess.vtgate.BatchQuery;
import com.youtube.vitess.vtgate.BatchQueryResponse;
import com.youtube.vitess.vtgate.Exceptions.ConnectionException;
import com.youtube.vitess.vtgate.Field;
import com.youtube.vitess.vtgate.Query;
import com.youtube.vitess.vtgate.QueryResponse;
import com.youtube.vitess.vtgate.QueryResult;
import com.youtube.vitess.vtgate.SplitQueryRequest;
import com.youtube.vitess.vtgate.SplitQueryResponse;
import com.youtube.vitess.vtgate.rpcclient.RpcClient;

public class GoRpcClient implements RpcClient {
  public static final Logger LOGGER = LogManager.getLogger(GoRpcClient.class.getName());
  public static final String BSON_RPC_PATH = "/_bson_rpc_";
  private Client client;

  public GoRpcClient(Client client) {
    this.client = client;
  }

  @Override
  public Object begin() throws ConnectionException {
    Response response = call("VTGate.Begin", new BasicBSONObject());
    return response.getReply();
  }

  @Override
  public QueryResponse execute(Query query) throws ConnectionException {
    String callMethod = null;
    Response response;
    if (query.isStreaming()) {
      if (query.getKeyspaceIds() != null) {
        callMethod = "VTGate.StreamExecuteKeyspaceIds";
      } else {
        callMethod = "VTGate.StreamExecuteKeyRanges";
      }
      response = streamCall(callMethod, Bsonify.queryToBson(query));
    } else {
      if (query.getKeyspaceIds() != null) {
        callMethod = "VTGate.ExecuteKeyspaceIds";
      } else {
        callMethod = "VTGate.ExecuteKeyRanges";
      }
      response = call(callMethod, Bsonify.queryToBson(query));
    }
    return Bsonify.bsonToQueryResponse((BSONObject) response.getReply());
  }

  @Override
  public QueryResult streamNext(List<Field> fields) throws ConnectionException {
    Response response;
    try {
      response = client.streamNext();
    } catch (GoRpcException | ApplicationException e) {
      LOGGER.error("vtgate exception", e);
      throw new ConnectionException("vtgate exception: " + e.getMessage());
    }

    if (response == null) {
      return null;
    }
    BSONObject reply = (BSONObject) response.getReply();
    if (reply.containsField("Result")) {
      BSONObject result = (BSONObject) reply.get("Result");
      return Bsonify.bsonToQueryResult(result, fields);
    }
    return null;
  }

  @Override
  public BatchQueryResponse batchExecute(BatchQuery batchQuery) throws ConnectionException {
    String callMethod = "VTGate.ExecuteBatchKeyspaceIds";
    Response response = call(callMethod, Bsonify.batchQueryToBson(batchQuery));
    return Bsonify.bsonToBatchQueryResponse((BSONObject) response.getReply());
  }

  @Override
  public void commit(Object session) throws ConnectionException {
    call("VTGate.Commit", session);
  }

  @Override
  public void rollback(Object session) throws ConnectionException {
    call("VTGate.Rollback", session);
  }

  @Override
  public void close() throws ConnectionException {
    try {
      client.close();
    } catch (GoRpcException e) {
      LOGGER.error("vtgate exception", e);
      throw new ConnectionException("vtgate exception: " + e.getMessage());
    }
  }

  @Override
  public SplitQueryResponse splitQuery(SplitQueryRequest request) throws ConnectionException {
    String callMethod = "VTGate.GetMRSplits";
    Response response = call(callMethod, Bsonify.splitQueryRequestToBson(request));
    return Bsonify.bsonToSplitQueryResponse((BSONObject) response.getReply());
  }

  private Response call(String methodName, Object args) throws ConnectionException {
    try {
      Response response = client.call(methodName, args);
      return response;
    } catch (GoRpcException | ApplicationException e) {
      LOGGER.error("vtgate exception", e);
      throw new ConnectionException("vtgate exception: " + e.getMessage());
    }
  }

  private Response streamCall(String methodName, Object args) throws ConnectionException {
    try {
      client.streamCall(methodName, args);
      return client.streamNext();
    } catch (GoRpcException | ApplicationException e) {
      LOGGER.error("vtgate exception", e);
      throw new ConnectionException("vtgate exception: " + e.getMessage());
    }
  }
}
