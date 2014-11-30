package com.youtube.vitess.vtgate;

import com.youtube.vitess.vtgate.Exceptions.ConnectionException;
import com.youtube.vitess.vtgate.Exceptions.DatabaseException;
import com.youtube.vitess.vtgate.Exceptions.IntegrityException;
import com.youtube.vitess.vtgate.cursor.Cursor;
import com.youtube.vitess.vtgate.cursor.CursorImpl;
import com.youtube.vitess.vtgate.cursor.StreamCursor;
import com.youtube.vitess.vtgate.rpcclient.RpcClient;
import com.youtube.vitess.vtgate.rpcclient.RpcClientFactory;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;

/**
 * A single threaded VtGate client
 *
 * Usage:
 *
 * <pre>
 *VtGate vtGate = VtGate.connect(addresses);
 *Query query = new QueryBuilder()...add params...build();
 *Cursor cursor = vtGate.execute(query);
 *for(Row row : cursor) {
 *		processRow(row);
 *}
 *
 *For DMLs
 *vtgate.begin();
 *Query query = new QueryBuilder()...add params...build();
 *vtgate.execute(query);
 *vtgate.commit();
 *
 *vtgate.close();
 *</pre>
 *
 */
public class VtGate {
  private static String INTEGRITY_ERROR_MSG = "(errno 1062)";
  private RpcClient client;
  private Object session;

  /**
   * Opens connection to a VtGate server. Connection remains open until close() is called.
   *
   * @param addresses comma separated list of host:port pairs
   * @param timeoutMs connection timeout in milliseconds, 0 for no timeout
   * @throws ConnectionException
   */
  public static VtGate connect(String addresses, int timeoutMs) throws ConnectionException {
    List<String> addressList = Arrays.asList(addresses.split(","));
    int index = new Random().nextInt(addressList.size());
    RpcClient client = RpcClientFactory.get(addressList.get(index), timeoutMs);
    return new VtGate(client);
  }

  private VtGate(RpcClient client) {
    this.client = client;
  }

  public void begin() throws ConnectionException {
    session = client.begin();
  }

  public Cursor execute(Query query) throws DatabaseException, ConnectionException {
    if (session != null) {
      query.setSession(session);
    }
    QueryResponse response = client.execute(query);
    String error = response.getError();
    if (error != null) {
      if (error.contains(INTEGRITY_ERROR_MSG)) {
        throw new IntegrityException(error);
      }
      throw new DatabaseException(response.getError());
    }
    if (response.getSession() != null) {
      session = response.getSession();
    }
    if (query.isStreaming()) {
      return new StreamCursor(response.getResult(), client);
    }
    return new CursorImpl(response.getResult());
  }

  public List<Cursor> execute(BatchQuery query) throws DatabaseException, ConnectionException {
    if (session != null) {
      query.setSession(session);
    }
    BatchQueryResponse response = client.batchExecute(query);
    String error = response.getError();
    if (error != null) {
      if (error.contains(INTEGRITY_ERROR_MSG)) {
        throw new IntegrityException(error);
      }
      throw new DatabaseException(response.getError());
    }
    if (response.getSession() != null) {
      session = response.getSession();
    }
    List<Cursor> cursors = new LinkedList<>();
    for (QueryResult qr : response.getResults()) {
      cursors.add(new CursorImpl(qr));
    }
    return cursors;
  }

  /**
   * Split a query into primary key range query parts. Rows corresponding to the sub queries will
   * add up to original queries' rows. Sub queries are by default built to run against 'rdonly'
   * instances. Batch jobs or MapReduce jobs that needs to scan all rows can use these queries to
   * parallelize full table scans.
   */
  public Map<Query, Long> splitQuery(String keyspace, String sql, int splitCount)
      throws ConnectionException, DatabaseException {
    SplitQueryRequest req = new SplitQueryRequest(sql, keyspace, splitCount);
    SplitQueryResponse response = client.splitQuery(req);
    if (response.getError() != null) {
      throw new DatabaseException(response.getError());
    }
    return response.getQueries();
  }

  public void commit() throws ConnectionException {
    try {
      client.commit(session);
    } finally {
      session = null;
    }
  }

  public void rollback() throws ConnectionException {
    try {
      client.rollback(session);
    } finally {
      session = null;
    }
  }

  public void close() throws ConnectionException {
    if (session != null) {
      rollback();
    }
    client.close();
  }
}
