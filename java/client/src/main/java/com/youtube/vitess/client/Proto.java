package com.youtube.vitess.client;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.primitives.UnsignedLong;
import com.google.protobuf.ByteString;

import com.youtube.vitess.client.cursor.Cursor;
import com.youtube.vitess.client.cursor.SimpleCursor;
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
   * Errors returned by Vitess are documented in the
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

  @SuppressWarnings("unchecked")
  private static void buildListBindVariable(BindVariable.Builder builder, Iterable<?> list) {
    Iterator<?> itr = list.iterator();

    if (itr.hasNext()) {
      // Check the type of the first item to determine the type of list.
      Object item = itr.next();

      if (item instanceof byte[]) {
        // List of Bytes
        builder.setType(BindVariable.Type.TYPE_BYTES_LIST);
        builder.addAllValueBytesList(
            Iterables.transform((Iterable<? extends byte[]>) list, BYTE_ARRAY_TO_BYTE_STRING));
      } else if (item instanceof Integer) {
        // List of Int32
        builder.setType(BindVariable.Type.TYPE_INT_LIST);
        builder.addValueIntList((Integer) item);
        while (itr.hasNext()) {
          builder.addValueIntList((Integer) itr.next());
        }
      } else if (item instanceof Long) {
        // List of Int64
        builder.setType(BindVariable.Type.TYPE_INT_LIST);
        builder.addAllValueIntList((Iterable<? extends Long>) list);
      } else if (item instanceof UnsignedLong) {
        // List of Uint64
        builder.setType(BindVariable.Type.TYPE_UINT_LIST);
        builder.addValueUintList(((UnsignedLong) item).longValue());
        while (itr.hasNext()) {
          builder.addValueUintList(((UnsignedLong) itr.next()).longValue());
        }
      } else if (item instanceof Float) {
        // List of Float
        builder.setType(BindVariable.Type.TYPE_FLOAT_LIST);
        builder.addValueFloatList((Float) item);
        while (itr.hasNext()) {
          builder.addValueFloatList((Float) itr.next());
        }
      } else if (item instanceof Double) {
        // List of Double
        builder.setType(BindVariable.Type.TYPE_FLOAT_LIST);
        builder.addAllValueFloatList((Iterable<? extends Double>) list);
      } else {
        throw new IllegalArgumentException("unsupported list bind var type");
      }
    } else {
      // The list bind var is empty. Due to type erasure, we have no way
      // of knowing what type of list it was meant to be. VTTablet will
      // reject an empty list anyway, so we'll just pretend it was a
      // List of Bytes.
      builder.setType(BindVariable.Type.TYPE_BYTES_LIST);
    }
  }

  public static BindVariable buildBindVariable(Object value) {
    BindVariable.Builder builder = BindVariable.newBuilder();

    if (value instanceof byte[]) {
      // Bytes
      builder.setType(BindVariable.Type.TYPE_BYTES);
      builder.setValueBytes(ByteString.copyFrom((byte[]) value));
    } else if (value instanceof Integer) {
      // Int32
      builder.setType(BindVariable.Type.TYPE_INT);
      builder.setValueInt((int) value);
    } else if (value instanceof Long) {
      // Int64
      builder.setType(BindVariable.Type.TYPE_INT);
      builder.setValueInt((long) value);
    } else if (value instanceof UnsignedLong) {
      // Uint64
      builder.setType(BindVariable.Type.TYPE_UINT);
      builder.setValueUint(((UnsignedLong) value).longValue());
    } else if (value instanceof Float) {
      // Float
      builder.setType(BindVariable.Type.TYPE_FLOAT);
      builder.setValueFloat((float) value);
    } else if (value instanceof Double) {
      // Double
      builder.setType(BindVariable.Type.TYPE_FLOAT);
      builder.setValueFloat((double) value);
    } else if (value instanceof Iterable<?>) {
      // List Bind Vars
      buildListBindVariable(builder, (Iterable<?>) value);
    } else {
      throw new IllegalArgumentException("unsupported bind var type");
    }

    return builder.build();
  }

  public static EntityId buildEntityId(byte[] keyspaceId, Object value) {
    EntityId.Builder builder = EntityId.newBuilder().setKeyspaceId(ByteString.copyFrom(keyspaceId));

    if (value instanceof byte[]) {
      // Bytes
      builder.setXidType(EntityId.Type.TYPE_BYTES);
      builder.setXidBytes(ByteString.copyFrom((byte[]) value));
    } else if (value instanceof Integer) {
      // Int32
      builder.setXidType(EntityId.Type.TYPE_INT);
      builder.setXidInt((int) value);
    } else if (value instanceof Long) {
      // Int64
      builder.setXidType(EntityId.Type.TYPE_INT);
      builder.setXidInt((long) value);
    } else if (value instanceof UnsignedLong) {
      // Uint64
      builder.setXidType(EntityId.Type.TYPE_UINT);
      builder.setXidUint(((UnsignedLong) value).longValue());
    } else if (value instanceof Float) {
      // Float
      builder.setXidType(EntityId.Type.TYPE_FLOAT);
      builder.setXidFloat((float) value);
    } else if (value instanceof Double) {
      // Double
      builder.setXidType(EntityId.Type.TYPE_FLOAT);
      builder.setXidFloat((double) value);
    } else {
      throw new IllegalArgumentException("unsupported entity ID type");
    }

    return builder.build();
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
}
