package com.github.youtube.vitess.jdbc.bson;

import com.google.common.primitives.Ints;
import com.google.protobuf.ByteString;
import com.google.protobuf.Message;

import com.github.youtube.vitess.jdbc.vtocc.QueryService.Cell;
import com.github.youtube.vitess.jdbc.vtocc.QueryService.CommitResponse;
import com.github.youtube.vitess.jdbc.vtocc.QueryService.Field;
import com.github.youtube.vitess.jdbc.vtocc.QueryService.Field.Type;
import com.github.youtube.vitess.jdbc.vtocc.QueryService.QueryResult;
import com.github.youtube.vitess.jdbc.vtocc.QueryService.QueryResult.Builder;
import com.github.youtube.vitess.jdbc.vtocc.QueryService.QueryResultList;
import com.github.youtube.vitess.jdbc.vtocc.QueryService.RollbackResponse;
import com.github.youtube.vitess.jdbc.vtocc.QueryService.Row;
import com.github.youtube.vitess.jdbc.vtocc.QueryService.SessionInfo;
import com.github.youtube.vitess.jdbc.vtocc.QueryService.TransactionInfo;

import org.bson.BSONObject;

import java.nio.charset.StandardCharsets;

/**
 * Converts {@link org.bson.BSONObject} to a {@link com.github.youtube.vitess.jdbc.vtocc.QueryService.SqlQuery}
 * response.
 *
 * @see #create(String, org.bson.BSONObject)
 */
public class SqlQueryResponseFactory {

  public Message create(String methodName, BSONObject body) {
    try {
      switch (methodName) {
        case "Rollback":
          return RollbackResponse.getDefaultInstance();
        case "Commit":
          return CommitResponse.getDefaultInstance();
        case "Begin":
          return createTransactionInfo(body);
        case "GetSessionId":
          return createSessionInfo(body);
        case "Execute":
          return createQueryResult(body);
        case "ExecuteBatch":
          return createQueryResultList(body);
        default:
          throw new IllegalArgumentException("Response ServiceMethod is not valid: " + methodName);
      }
    } catch (RuntimeException e) {
      throw new IllegalArgumentException("Can't parse response body for " + methodName, e);
    }
  }

  protected TransactionInfo createTransactionInfo(BSONObject body) {
    TransactionInfo.Builder transactionInfo = TransactionInfo.newBuilder();
    transactionInfo.setTransactionId((Long) body.get("TransactionId"));
    return transactionInfo.build();
  }

  protected SessionInfo createSessionInfo(BSONObject body) {
    SessionInfo.Builder sessionInfo = SessionInfo.newBuilder();
    sessionInfo.setSessionId((Long) body.get("SessionId"));
    return sessionInfo.build();
  }

  protected QueryResult createQueryResult(BSONObject bsonBody) {
    Builder queryResult = QueryResult.newBuilder();
    queryResult.setInsertId((Long) bsonBody.get("InsertId"));
    BSONObject fields = (BSONObject) bsonBody.get("Fields");
    for (String index : fields.keySet()) {
      BSONObject field = (BSONObject) fields.get(index);
      queryResult.addFields(
          Field.newBuilder()
              .setName(new String((byte[]) field.get("Name"), StandardCharsets.UTF_8))
              .setType(Type.valueOf(Ints.checkedCast((Long) field.get("Type"))))
              .build()
      );
    }

    BSONObject rows = (BSONObject) bsonBody.get("Rows");
    for (String rowIndex : rows.keySet()) {
      BSONObject bsonRow = (BSONObject) rows.get(rowIndex);
      Row.Builder queryRow = Row.newBuilder();
      for (String cellIndex : bsonRow.keySet()) {
        queryRow.addValues(Cell.newBuilder()
            .setValue(ByteString.copyFrom((byte[]) bsonRow.get(cellIndex))));
      }
      queryResult.addRows(queryRow);
    }

    queryResult.setRowsAffected((Long) bsonBody.get("RowsAffected"));
    return queryResult.build();
  }

  protected QueryResultList createQueryResultList(BSONObject body) {
    QueryResultList.Builder queryResultList = QueryResultList.newBuilder();
    for (String index : body.keySet()) {
      BSONObject query = (BSONObject) body.get(index);
      queryResultList.addList(createQueryResult(query));
    }
    return queryResultList.build();
  }

}
