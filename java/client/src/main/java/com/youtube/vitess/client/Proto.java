package com.youtube.vitess.client;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.primitives.UnsignedLong;
import com.google.protobuf.ByteString;

import com.youtube.vitess.client.cursor.Cursor;
import com.youtube.vitess.client.cursor.SimpleCursor;
import com.youtube.vitess.proto.Query;
import com.youtube.vitess.proto.Query.BindVariable;
import com.youtube.vitess.proto.Query.BoundQuery;
import com.youtube.vitess.proto.Query.QueryResult;
import com.youtube.vitess.proto.Vtgate.BoundKeyspaceIdQuery;
import com.youtube.vitess.proto.Vtgate.BoundShardQuery;
import com.youtube.vitess.proto.Vtgate.ExecuteEntityIdsRequest.EntityId;
import com.youtube.vitess.proto.Vtrpc.RPCError;

import java.sql.SQLException;
import java.sql.SQLIntegrityConstraintViolationException;
import java.sql.SQLInvalidAuthorizationSpecException;
import java.sql.SQLNonTransientException;
import java.sql.SQLSyntaxErrorException;
import java.sql.SQLTimeoutException;
import java.sql.SQLTransientException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Proto contains methods for working with Vitess protobuf messages.
 */
public class Proto {
  /**
   * Throws the proper SQLException for an error returned by VTGate.
   *
   * <p>Errors returned by Vitess are documented in the
   * <a href="https://github.com/youtube/vitess/blob/master/proto/vtrpc.proto">vtrpc proto</a>.
   */
  public static void checkError(RPCError error) throws SQLException {
    if (error != null) {
      switch (error.getCode()) {
        case SUCCESS:
          break;
        case BAD_INPUT:
          throw new SQLSyntaxErrorException(error.toString());
        case DEADLINE_EXCEEDED:
          throw new SQLTimeoutException(error.toString());
        case INTEGRITY_ERROR:
          throw new SQLIntegrityConstraintViolationException(error.toString());
        case TRANSIENT_ERROR:
          throw new SQLTransientException(error.toString());
        case UNAUTHENTICATED:
          throw new SQLInvalidAuthorizationSpecException(error.toString());
        default:
          throw new SQLNonTransientException("Vitess RPC error: " + error.toString());
      }
    }
  }

  public static BindVariable buildBindVariable(Object value) {
    BindVariable.Builder builder = BindVariable.newBuilder();

    if (value instanceof Iterable<?>) {
      // List Bind Vars
      Iterator<?> itr = ((Iterable<?>) value).iterator();

      if (!itr.hasNext()) {
        throw new IllegalArgumentException("Can't pass empty list as list bind variable.");
      }

      builder.setType(Query.Type.TUPLE);

      while (itr.hasNext()) {
        TypedValue tval = new TypedValue(itr.next());
        builder.addValues(Query.Value.newBuilder().setType(tval.type).setValue(tval.value).build());
      }
    } else {
      TypedValue tval = new TypedValue(value);
      builder.setType(tval.type);
      builder.setValue(tval.value);
    }

    return builder.build();
  }

  public static EntityId buildEntityId(byte[] keyspaceId, Object value) {
    TypedValue tval = new TypedValue(value);

    return EntityId.newBuilder()
        .setKeyspaceId(ByteString.copyFrom(keyspaceId))
        .setXidType(tval.type)
        .setXidValue(tval.value)
        .build();
  }

  /**
   * bindQuery creates a BoundQuery from query and vars.
   */
  public static BoundQuery bindQuery(String query, Map<String, ?> vars) {
    BoundQuery.Builder boundQueryBuilder = BoundQuery.newBuilder().setSql(query);
    if (vars != null) {
      Map<String, BindVariable> bindVars = boundQueryBuilder.getMutableBindVariables();
      for (Map.Entry<String, ?> entry : vars.entrySet()) {
        bindVars.put(entry.getKey(), buildBindVariable(entry.getValue()));
      }
    }
    return boundQueryBuilder.build();
  }

  /**
   * bindShardQuery creates a BoundShardQuery.
   */
  public static BoundShardQuery bindShardQuery(
      String keyspace, Iterable<String> shards, BoundQuery query) {
    return BoundShardQuery.newBuilder()
        .setKeyspace(keyspace)
        .addAllShards(shards)
        .setQuery(query)
        .build();
  }

  /**
   * bindShardQuery creates a BoundShardQuery.
   */
  public static BoundShardQuery bindShardQuery(
      String keyspace, Iterable<String> shards, String query, Map<String, ?> vars) {
    return bindShardQuery(keyspace, shards, bindQuery(query, vars));
  }

  /**
   * bindKeyspaceIdQuery creates a BoundKeyspaceIdQuery.
   */
  public static BoundKeyspaceIdQuery bindKeyspaceIdQuery(
      String keyspace, Iterable<byte[]> keyspaceIds, BoundQuery query) {
    return BoundKeyspaceIdQuery.newBuilder()
        .setKeyspace(keyspace)
        .addAllKeyspaceIds(Iterables.transform(keyspaceIds, BYTE_ARRAY_TO_BYTE_STRING))
        .setQuery(query)
        .build();
  }

  /**
   * bindKeyspaceIdQuery creates a BoundKeyspaceIdQuery.
   */
  public static BoundKeyspaceIdQuery bindKeyspaceIdQuery(
      String keyspace, Iterable<byte[]> keyspaceIds, String query, Map<String, ?> vars) {
    return bindKeyspaceIdQuery(keyspace, keyspaceIds, bindQuery(query, vars));
  }

  public static List<Cursor> toCursorList(List<QueryResult> queryResults) {
    ImmutableList.Builder<Cursor> builder = new ImmutableList.Builder<Cursor>();
    for (QueryResult queryResult : queryResults) {
      builder.add(new SimpleCursor(queryResult));
    }
    return builder.build();
  }

  public static final Function<byte[], ByteString> BYTE_ARRAY_TO_BYTE_STRING =
      new Function<byte[], ByteString>() {
        @Override
        public ByteString apply(byte[] from) {
          return ByteString.copyFrom(from);
        }
      };

  public static final Function<Map.Entry<byte[], ?>, EntityId> MAP_ENTRY_TO_ENTITY_KEYSPACE_ID =
      new Function<Map.Entry<byte[], ?>, EntityId>() {
        @Override
        public EntityId apply(Map.Entry<byte[], ?> entry) {
          return buildEntityId(entry.getKey(), entry.getValue());
        }
      };

  /**
   * Represents a type and value in the type system used in query.proto.
   */
  protected static class TypedValue {
    Query.Type type;
    ByteString value;

    TypedValue(Object value) {
      if (value == null) {
        this.type = Query.Type.NULL_TYPE;
      } else if (value instanceof String) {
        // String
        this.type = Query.Type.VARCHAR;
        this.value = ByteString.copyFromUtf8((String) value);
      } else if (value instanceof byte[]) {
        // Bytes
        this.type = Query.Type.VARBINARY;
        this.value = ByteString.copyFrom((byte[]) value);
      } else if (value instanceof Integer || value instanceof Long) {
        // Int32, Int64
        this.type = Query.Type.INT64;
        this.value = ByteString.copyFromUtf8(value.toString());
      } else if (value instanceof UnsignedLong) {
        // Uint64
        this.type = Query.Type.UINT64;
        this.value = ByteString.copyFromUtf8(value.toString());
      } else if (value instanceof Float || value instanceof Double) {
        // Float, Double
        this.type = Query.Type.FLOAT64;
        this.value = ByteString.copyFromUtf8(value.toString());
      } else {
        throw new IllegalArgumentException(
            "unsupported type for Query.Value proto: " + value.getClass());
      }
    }
  }
}
