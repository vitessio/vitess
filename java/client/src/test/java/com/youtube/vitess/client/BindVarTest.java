package com.youtube.vitess.client;

import static org.junit.Assert.assertEquals;

import com.google.common.primitives.UnsignedLong;
import com.google.protobuf.ByteString;

import com.youtube.vitess.proto.Query.BindVariable;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import java.util.Arrays;
import java.util.Collection;

@RunWith(value = Parameterized.class)
public class BindVarTest {
  private Object input;
  private BindVariable expected;

  @Parameters
  public static Collection<Object[]> testParams() {
    Object[][] params = {
        // Bytes
        {new byte[] {1, 2, 3}, BindVariable.newBuilder()
                                   .setType(BindVariable.Type.TYPE_BYTES)
                                   .setValueBytes(ByteString.copyFrom(new byte[] {1, 2, 3}))
                                   .build()},
        // Int
        {123, BindVariable.newBuilder()
                  .setType(BindVariable.Type.TYPE_INT)
                  .setValueInt(123)
                  .build()},
        {123L, BindVariable.newBuilder()
                   .setType(BindVariable.Type.TYPE_INT)
                   .setValueInt(123)
                   .build()},
        // Uint
        {UnsignedLong.fromLongBits(-1), BindVariable.newBuilder()
                                            .setType(BindVariable.Type.TYPE_UINT)
                                            .setValueUint(-1)
                                            .build()},
        // Float
        {1.23f, BindVariable.newBuilder()
                    .setType(BindVariable.Type.TYPE_FLOAT)
                    .setValueFloat(1.23f)
                    .build()},
        {1.23, BindVariable.newBuilder()
                   .setType(BindVariable.Type.TYPE_FLOAT)
                   .setValueFloat(1.23)
                   .build()},
        // List of Bytes
        {Arrays.asList(new byte[] {1, 2, 3}, new byte[] {4, 5, 6}),
         BindVariable.newBuilder()
             .setType(BindVariable.Type.TYPE_BYTES_LIST)
             .addValueBytesList(ByteString.copyFrom(new byte[] {1, 2, 3}))
             .addValueBytesList(ByteString.copyFrom(new byte[] {4, 5, 6}))
             .build()},
        // List of Int
        {Arrays.asList(1, 2, 3), BindVariable.newBuilder()
                                     .setType(BindVariable.Type.TYPE_INT_LIST)
                                     .addValueIntList(1)
                                     .addValueIntList(2)
                                     .addValueIntList(3)
                                     .build()},
        {Arrays.asList(1L, 2L, 3L), BindVariable.newBuilder()
                                        .setType(BindVariable.Type.TYPE_INT_LIST)
                                        .addAllValueIntList(Arrays.asList(1L, 2L, 3L))
                                        .build()},
        // List of Uint
        {Arrays.asList(UnsignedLong.fromLongBits(1), UnsignedLong.fromLongBits(2),
             UnsignedLong.fromLongBits(3)),
         BindVariable.newBuilder()
             .setType(BindVariable.Type.TYPE_UINT_LIST)
             .addAllValueUintList(Arrays.asList(1L, 2L, 3L))
             .build()},
        // List of Float
        {Arrays.asList(1.2f, 3.4f, 5.6f), BindVariable.newBuilder()
                                              .setType(BindVariable.Type.TYPE_FLOAT_LIST)
                                              .addValueFloatList(1.2f)
                                              .addValueFloatList(3.4f)
                                              .addValueFloatList(5.6f)
                                              .build()},
        {Arrays.asList(1.2, 3.4, 5.6), BindVariable.newBuilder()
                                           .setType(BindVariable.Type.TYPE_FLOAT_LIST)
                                           .addAllValueFloatList(Arrays.asList(1.2, 3.4, 5.6))
                                           .build()},
        // Empty List
        {Arrays.asList(), BindVariable.newBuilder()
                              .setType(BindVariable.Type.TYPE_BYTES_LIST)
                              .build()

        }};
    return Arrays.asList(params);
  }

  public BindVarTest(Object input, BindVariable expected) {
    this.input = input;
    this.expected = expected;
  }

  @Test
  public void testBuildBindVariable() {
    assertEquals(expected, Proto.buildBindVariable(input));
  }
}
