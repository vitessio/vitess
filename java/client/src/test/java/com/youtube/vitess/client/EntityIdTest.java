package com.youtube.vitess.client;

import static org.junit.Assert.assertEquals;

import com.google.common.primitives.UnsignedLong;
import com.google.protobuf.ByteString;

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

  @Parameters
  public static Collection<Object[]> testParams() {
    Object[][] params = {
        // Bytes
        {new byte[] {1, 2, 3}, EntityId.newBuilder()
                                   .setXidType(EntityId.Type.TYPE_BYTES)
                                   .setXidBytes(ByteString.copyFrom(new byte[] {1, 2, 3}))
                                   .build()},
        // Int
        {123, EntityId.newBuilder().setXidType(EntityId.Type.TYPE_INT).setXidInt(123).build()},
        {123L, EntityId.newBuilder().setXidType(EntityId.Type.TYPE_INT).setXidInt(123).build()},
        // Uint
        {UnsignedLong.fromLongBits(-1), EntityId.newBuilder()
                                            .setXidType(EntityId.Type.TYPE_UINT)
                                            .setXidUint(-1)
                                            .build()},
        // Float
        {1.23f, EntityId.newBuilder()
                    .setXidType(EntityId.Type.TYPE_FLOAT)
                    .setXidFloat(1.23f)
                    .build()},
        {1.23, EntityId.newBuilder()
                   .setXidType(EntityId.Type.TYPE_FLOAT)
                   .setXidFloat(1.23)
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
    assertEquals(expected, Proto.buildEntityId(input));
  }
}
