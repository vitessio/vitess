package com.youtube.vitess.vtgate.rpcclient.gorpc;

import com.google.common.primitives.Ints;
import com.google.common.primitives.UnsignedLong;

import com.youtube.vitess.vtgate.BatchQuery;
import com.youtube.vitess.vtgate.BatchQueryResponse;
import com.youtube.vitess.vtgate.BindVariable;
import com.youtube.vitess.vtgate.Field;
import com.youtube.vitess.vtgate.FieldType;
import com.youtube.vitess.vtgate.KeyRange;
import com.youtube.vitess.vtgate.KeyspaceId;
import com.youtube.vitess.vtgate.Query;
import com.youtube.vitess.vtgate.Query.QueryBuilder;
import com.youtube.vitess.vtgate.QueryResponse;
import com.youtube.vitess.vtgate.QueryResult;
import com.youtube.vitess.vtgate.Row;
import com.youtube.vitess.vtgate.Row.Cell;
import com.youtube.vitess.vtgate.SplitQueryRequest;
import com.youtube.vitess.vtgate.SplitQueryResponse;

import org.apache.commons.codec.binary.Hex;
import org.bson.BSONObject;
import org.bson.BasicBSONObject;
import org.bson.types.BasicBSONList;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class Bsonify {
  public static BSONObject queryToBson(Query query) {
    BSONObject b = new BasicBSONObject();
    b.put("Sql", query.getSql());
    b.put("Keyspace", query.getKeyspace());
    b.put("TabletType", query.getTabletType());
    if (query.getBindVars() != null) {
      b.put("BindVariables", bindVarsToBSON(query.getBindVars()));
    }
    if (query.getKeyspaceIds() != null) {
      b.put("KeyspaceIds", query.getKeyspaceIds());
    } else {
      b.put("KeyRanges", query.getKeyRanges());
    }
    if (query.getSession() != null) {
      b.put("Session", query.getSession());
    }
    return b;
  }

  public static BSONObject bindVarsToBSON(List<BindVariable> bindVars) {
    BSONObject bindVariables = new BasicBSONObject();
    for (BindVariable b : bindVars) {
      if (b.getType().equals(BindVariable.Type.NULL)) {
        bindVariables.put(b.getName(), null);
      }
      if (b.getType().equals(BindVariable.Type.LONG)) {
        bindVariables.put(b.getName(), b.getLongVal());
      }
      if (b.getType().equals(BindVariable.Type.UNSIGNED_LONG)) {
        bindVariables.put(b.getName(), b.getULongVal());
      }
      if (b.getType().equals(BindVariable.Type.DOUBLE)) {
        bindVariables.put(b.getName(), b.getDoubleVal());
      }
      if (b.getType().equals(BindVariable.Type.BYTE_ARRAY)) {
        bindVariables.put(b.getName(), b.getByteArrayVal());
      }
    }
    return bindVariables;
  }

  public static BSONObject batchQueryToBson(BatchQuery batchQuery) {
    BSONObject b = new BasicBSONObject();
    List<Map<String, Object>> queries = new LinkedList<>();
    Iterator<String> sqlIter = batchQuery.getSqls().iterator();
    Iterator<List<BindVariable>> bvIter = batchQuery.getBindVarsList().iterator();
    while (sqlIter.hasNext()) {
      Map<String, Object> query = new HashMap<>();
      query.put("Sql", sqlIter.next());
      List<BindVariable> bindVars = bvIter.next();
      if (bindVars != null) {
        query.put("BindVariables", bindVarsToBSON(bindVars));
      }
      queries.add(query);
    }
    b.put("Queries", queries);
    b.put("Keyspace", batchQuery.getKeyspace());
    b.put("TabletType", batchQuery.getTabletType());
    b.put("KeyspaceIds", batchQuery.getKeyspaceIds());
    if (batchQuery.getSession() != null) {
      b.put("Session", batchQuery.getSession());
    }
    return b;
  }

  public static QueryResponse bsonToQueryResponse(BSONObject reply) {
    String error = null;
    if (reply.containsField("Error")) {
      byte[] err = (byte[]) reply.get("Error");
      if (err.length > 0) {
        error = new String(err);
      }
    }

    QueryResult queryResult = null;
    BSONObject result = (BSONObject) reply.get("Result");
    if (result != null) {
      queryResult = bsonToQueryResult(result, null);
    }

    Object session = null;
    if (reply.containsField("Session")) {
      session = reply.get("Session");
    }
    return new QueryResponse(queryResult, session, error);
  }

  public static BatchQueryResponse bsonToBatchQueryResponse(BSONObject reply) {
    String error = null;
    if (reply.containsField("Error")) {
      byte[] err = (byte[]) reply.get("Error");
      if (err.length > 0) {
        error = new String(err);
      }
    }
    Object session = null;
    if (reply.containsField("Session")) {
      session = reply.get("Session");
    }
    List<QueryResult> qrs = new LinkedList<>();
    BasicBSONList results = (BasicBSONList) reply.get("List");
    for (Object result : results) {
      qrs.add(bsonToQueryResult((BSONObject) result, null));
    }
    return new BatchQueryResponse(qrs, session, error);
  }

  public static QueryResult bsonToQueryResult(BSONObject result, List<Field> fields) {
    if (fields == null) {
      fields = bsonToFields(result);
    }
    List<Row> rows = bsonToRows(result, fields);
    long rowsAffected = ((UnsignedLong) result.get("RowsAffected")).longValue();
    long lastRowId = ((UnsignedLong) result.get("InsertId")).longValue();
    return new QueryResult(rows, fields, rowsAffected, lastRowId);
  }

  public static List<Field> bsonToFields(BSONObject result) {
    List<Field> fieldList = new LinkedList<>();
    BasicBSONList fields = (BasicBSONList) result.get("Fields");
    for (Object field : fields) {
      BSONObject fieldBson = (BSONObject) field;
      String fieldName = new String((byte[]) fieldBson.get("Name"));
      int mysqlType = Ints.checkedCast((Long) fieldBson.get("Type"));
      FieldType fieldType = FieldType.get(mysqlType);
      fieldList.add(new Field(fieldName, fieldType));
    }
    return fieldList;
  }

  public static List<Row> bsonToRows(BSONObject result, List<Field> fields) {
    List<Row> rowList = new LinkedList<>();
    BasicBSONList rows = (BasicBSONList) result.get("Rows");
    for (Object row : rows) {
      LinkedList<Cell> cells = new LinkedList<>();
      BasicBSONList cols = (BasicBSONList) row;
      Iterator<Field> fieldsIter = fields.iterator();
      for (Object col : cols) {
        byte[] val = col != null ? (byte[]) col : null;
        Field field = fieldsIter.next();
        FieldType ft = field.getType();
        cells.add(new Cell(field.getName(), ft.convert(val), ft.javaType));
      }
      rowList.add(new Row(cells));
    }
    return rowList;
  }

  public static BSONObject splitQueryRequestToBson(SplitQueryRequest request) {
    BSONObject query = new BasicBSONObject();
    query.put("Sql", request.getSql());
    BSONObject b = new BasicBSONObject();
    b.put("Keyspace", request.getKeyspace());
    b.put("Query", query);
    b.put("SplitsPerShard", request.getSplitsPerShard());
    return b;
  }

  public static SplitQueryResponse bsonToSplitQueryResponse(BSONObject reply) {
    String error = null;
    if (reply.containsField("Error")) {
      byte[] err = (byte[]) reply.get("Error");
      if (err.length > 0) {
        error = new String(err);
      }
    }
    BasicBSONList result = (BasicBSONList) reply.get("Splits");
    Map<Query, Long> queries = new HashMap<>();
    for (Object split : result) {
      BSONObject splitObj = (BasicBSONObject) split;
      BSONObject query = (BasicBSONObject) (splitObj.get("Query"));
      String sql = new String((byte[]) query.get("Sql"));
      BSONObject bindVars = (BasicBSONObject) query.get("BindVariables");
      List<BindVariable> bindVariables = new LinkedList<>();
      for (String key : bindVars.keySet()) {
        BindVariable bv = null;
        Object val = bindVars.get(key);
        if (val == null) {
          bv = BindVariable.forNull(key);
        }
        if (val instanceof UnsignedLong) {
          bv = BindVariable.forULong(key, (UnsignedLong) val);
        }
        if (val instanceof Long) {
          bv = BindVariable.forLong(key, (Long) val);
        }
        if (val instanceof Double) {
          bv = BindVariable.forDouble(key, (Double) val);
        }
        if (val instanceof byte[]) {
          bv = BindVariable.forBytes(key, (byte[]) val);
        }
        if (bv == null) {
          throw new RuntimeException("invalid bind variable type: " + val.getClass());
        }
        bindVariables.add(bv);
      }
      String keyspace = new String((byte[]) query.get("Keyspace"));
      String tabletType = new String((byte[]) query.get("TabletType"));
      List<KeyRange> keyranges = new ArrayList<>();
      for (Object o : (List<?>) query.get("KeyRanges")) {
        BSONObject keyrange = (BasicBSONObject) o;
        String start = Hex.encodeHexString((byte[]) keyrange.get("Start"));
        String end = Hex.encodeHexString((byte[]) keyrange.get("End"));
        KeyRange kr = new KeyRange(KeyspaceId.valueOf(start), KeyspaceId.valueOf(end));
        keyranges.add(kr);
      }

      Query q =
          new QueryBuilder(sql, keyspace, tabletType).setKeyRanges(keyranges)
              .setBindVars(bindVariables).setStreaming(true).build();
      long size = (long) splitObj.get("Size");
      queries.put(q, size);
    }
    return new SplitQueryResponse(queries, error);
  }
}
