package com.github.youtube.vitess.jdbc.bson;

import com.google.protobuf.Message;

import com.github.youtube.vitess.jdbc.vtocc.QueryService.BindVariable;
import com.github.youtube.vitess.jdbc.vtocc.QueryService.BoundQuery;
import com.github.youtube.vitess.jdbc.vtocc.QueryService.Query;
import com.github.youtube.vitess.jdbc.vtocc.QueryService.QueryList;
import com.github.youtube.vitess.jdbc.vtocc.QueryService.Session;
import com.github.youtube.vitess.jdbc.vtocc.QueryService.SessionParams;

import org.bson.BSONObject;
import org.bson.BasicBSONObject;
import org.bson.types.BasicBSONList;

import java.util.List;

/**
 * Factory to create {@link org.bson.BSONObject} request body from a {@link
 * com.github.youtube.vitess.jdbc.vtocc.QueryService.SqlQuery} request.
 *
 * @see #create(com.google.protobuf.Message)
 */
public class SqlQueryBsonRequestBodyFactory {

  public BSONObject create(Message message) {
    if (message instanceof Query) {
      return create((Query) message);
    } else if (message instanceof SessionParams) {
      return create((SessionParams) message);
    } else if (message instanceof QueryList) {
      return create((QueryList) message);
    } else if (message instanceof Session) {
      return create((Session) message);
    } else {
      throw new IllegalArgumentException("Can not parse message " + message.getClass());
    }
  }

  protected BSONObject create(Query query) {
    BSONObject bsonMessage = new BasicBSONObject();
    bsonMessage.put("TransactionId", query.getSession().getTransactionId());
    bsonMessage.put("SessionId", query.getSession().getSessionId());
    bsonMessage.put("BindVariables", bindVariablesToMap(query.getBindVariablesList()));
    bsonMessage.put("Sql", query.getSql().toByteArray());
    return bsonMessage;
  }

  protected BSONObject bindVariablesToMap(List<BindVariable> bindVariableList) {
    BSONObject bsonBindVariables = new BasicBSONObject();
    for (BindVariable bindVariable : bindVariableList) {
      addTypeField(bsonBindVariables, bindVariable);
    }
    return bsonBindVariables;
  }

  protected void addTypeField(BSONObject bsonBindVariables, BindVariable bindVariable) {
    switch (bindVariable.getType()) {
      case BYTES:
        bsonBindVariables.put(bindVariable.getName(), bindVariable.getValueBytes().toStringUtf8());
        break;
      case FLOAT:
        bsonBindVariables.put(bindVariable.getName(), bindVariable.getValueFloat());
        break;
      case INT:
        bsonBindVariables.put(bindVariable.getName(), bindVariable.getValueInt());
        break;
      case UINT:
        bsonBindVariables.put(bindVariable.getName(), bindVariable.getValueUint());
        break;
      case NULL:
        bsonBindVariables.put(bindVariable.getName(), 0);
        break;
      default:
        break;
    }
  }

  protected BSONObject create(SessionParams sessionParams) {
    BSONObject bsonMessage = new BasicBSONObject();
    bsonMessage.put("Keyspace", sessionParams.getKeyspace());
    bsonMessage.put("Shard", sessionParams.getShard());
    return bsonMessage;
  }

  protected BSONObject create(QueryList queryList) {
    BSONObject bsonQueryList = new BasicBSONObject();
    bsonQueryList.put("TransactionId", queryList.getSession().getTransactionId());
    bsonQueryList.put("SessionId", queryList.getSession().getSessionId());

    BasicBSONList bsonQueries = new BasicBSONList();
    for (int i = 0; i < queryList.getQueriesCount(); i++) {
      BoundQuery boundQuery = queryList.getQueries(i);
      BSONObject bsonQuery = new BasicBSONObject();
      bsonQuery.put("BindVariables", bindVariablesToMap(boundQuery.getBindVariablesList()));
      bsonQuery.put("Sql", boundQuery.getSql().toStringUtf8());
      bsonQueries.put(i, bsonQuery);
    }

    bsonQueryList.put("Queries", bsonQueries);
    return bsonQueryList;

  }

  protected BSONObject create(Session session) {
    BSONObject bsonMessage = new BasicBSONObject();
    bsonMessage.put("TransactionId", session.getTransactionId());
    bsonMessage.put("SessionId", session.getSessionId());
    return bsonMessage;
  }
}

