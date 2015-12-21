package com.youtube.vitess.client;

import static org.junit.Assert.assertEquals;

import com.google.common.primitives.UnsignedLong;
import com.google.protobuf.ByteString;

import com.youtube.vitess.proto.Query;
import com.youtube.vitess.proto.Vtgate.ExecuteEntityIdsRequest.EntityId;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import java.util.Arrays;
import java.util.Collection;

@RunWith(value = Parameterized.class)
public class EntityIdTest {
  private Object input;
  private EntityId expected;

  private static final ByteString KEYSPACE_ID = ByteString.copyFrom(new byte[] {1, 2, 3});

  @Parameters
  public static Collection<Object[]> testParams() {
    Object[][] params = {
        // String
        {"hello world", EntityId.newBuilder()
                            .setKeyspaceId(KEYSPACE_ID)
                            .setXidType(Query.Type.VARCHAR)
                            .setXidValue(ByteString.copyFromUtf8("hello world"))
                            .build()},
        // Bytes
        {new byte[] {1, 2, 3}, EntityId.newBuilder()
                                   .setKeyspaceId(KEYSPACE_ID)
                                   .setXidType(Query.Type.VARBINARY)
                                   .setXidValue(ByteString.copyFrom(new byte[] {1, 2, 3}))
                                   .build()},
        // Int
        {123, EntityId.newBuilder()
                  .setKeyspaceId(KEYSPACE_ID)
                  .setXidType(Query.Type.INT64)
                  .setXidValue(ByteString.copyFromUtf8("123"))
                  .build()},
        {123L, EntityId.newBuilder()
                   .setKeyspaceId(KEYSPACE_ID)
                   .setXidType(Query.Type.INT64)
                   .setXidValue(ByteString.copyFromUtf8("123"))
                   .build()},
        // Uint
        {UnsignedLong.fromLongBits(-1), EntityId.newBuilder()
                                            .setKeyspaceId(KEYSPACE_ID)
                                            .setXidType(Query.Type.UINT64)
                                            .setXidValue(
                                                ByteString.copyFromUtf8("18446744073709551615"))
                                            .build()},
        // Float
        {1.23f, EntityId.newBuilder()
                    .setKeyspaceId(KEYSPACE_ID)
                    .setXidType(Query.Type.FLOAT64)
                    .setXidValue(ByteString.copyFromUtf8("1.23"))
                    .build()},
        {1.23, EntityId.newBuilder()
                   .setKeyspaceId(KEYSPACE_ID)
                   .setXidType(Query.Type.FLOAT64)
                   .setXidValue(ByteString.copyFromUtf8("1.23"))
                   .build()},
    };
    return Arrays.asList(params);
  }

  public EntityIdTest(Object input, EntityId expected) {
    this.input = input;
    this.expected = expected;
  }

  @Test
  public void testBuildEntityId() {
    assertEquals(expected, Proto.buildEntityId(KEYSPACE_ID.toByteArray(), input));
  }
}
