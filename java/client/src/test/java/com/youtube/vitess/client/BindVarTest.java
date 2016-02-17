package com.youtube.vitess.client;

import static org.junit.Assert.assertEquals;

import com.google.common.primitives.UnsignedLong;
import com.google.protobuf.ByteString;

import com.youtube.vitess.proto.Query;
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
        // String
        {"hello world", BindVariable.newBuilder()
                            .setType(Query.Type.VARCHAR)
                            .setValue(ByteString.copyFromUtf8("hello world"))
                            .build()},
        // Bytes
        {new byte[] {1, 2, 3}, BindVariable.newBuilder()
                                   .setType(Query.Type.VARBINARY)
                                   .setValue(ByteString.copyFrom(new byte[] {1, 2, 3}))
                                   .build()},
        // Int
        {123, BindVariable.newBuilder()
                  .setType(Query.Type.INT64)
                  .setValue(ByteString.copyFromUtf8("123"))
                  .build()},
        {123L, BindVariable.newBuilder()
                   .setType(Query.Type.INT64)
                   .setValue(ByteString.copyFromUtf8("123"))
                   .build()},
        // Uint
        {UnsignedLong.fromLongBits(-1), BindVariable.newBuilder()
                                            .setType(Query.Type.UINT64)
                                            .setValue(
                                                ByteString.copyFromUtf8("18446744073709551615"))
                                            .build()},
        // Float
        {1.23f, BindVariable.newBuilder()
                    .setType(Query.Type.FLOAT64)
                    .setValue(ByteString.copyFromUtf8("1.23"))
                    .build()},
        {1.23, BindVariable.newBuilder()
                   .setType(Query.Type.FLOAT64)
                   .setValue(ByteString.copyFromUtf8("1.23"))
                   .build()},
        // List of Bytes
        {Arrays.asList(new byte[] {1, 2, 3}, new byte[] {4, 5, 6}),
         BindVariable.newBuilder()
             .setType(Query.Type.TUPLE)
             .addValues(
                 Query.Value.newBuilder()
                     .setType(Query.Type.VARBINARY)
                     .setValue(ByteString.copyFrom(new byte[] {1, 2, 3}))
                     .build())
             .addValues(
                 Query.Value.newBuilder()
                     .setType(Query.Type.VARBINARY)
                     .setValue(ByteString.copyFrom(new byte[] {4, 5, 6}))
                     .build())
             .build()},
        // List of Int
        {Arrays.asList(1, 2, 3), BindVariable.newBuilder()
                                     .setType(Query.Type.TUPLE)
                                     .addValues(
                                         Query.Value.newBuilder()
                                             .setType(Query.Type.INT64)
                                             .setValue(ByteString.copyFromUtf8("1"))
                                             .build())
                                     .addValues(
                                         Query.Value.newBuilder()
                                             .setType(Query.Type.INT64)
                                             .setValue(ByteString.copyFromUtf8("2"))
                                             .build())
                                     .addValues(
                                         Query.Value.newBuilder()
                                             .setType(Query.Type.INT64)
                                             .setValue(ByteString.copyFromUtf8("3"))
                                             .build())
                                     .build()},
        {Arrays.asList(1L, 2L, 3L), BindVariable.newBuilder()
                                        .setType(Query.Type.TUPLE)
                                        .addValues(
                                            Query.Value.newBuilder()
                                                .setType(Query.Type.INT64)
                                                .setValue(ByteString.copyFromUtf8("1"))
                                                .build())
                                        .addValues(
                                            Query.Value.newBuilder()
                                                .setType(Query.Type.INT64)
                                                .setValue(ByteString.copyFromUtf8("2"))
                                                .build())
                                        .addValues(
                                            Query.Value.newBuilder()
                                                .setType(Query.Type.INT64)
                                                .setValue(ByteString.copyFromUtf8("3"))
                                                .build())
                                        .build()},
        // List of Uint
        {Arrays.asList(UnsignedLong.fromLongBits(1), UnsignedLong.fromLongBits(2),
             UnsignedLong.fromLongBits(3)),
         BindVariable.newBuilder()
             .setType(Query.Type.TUPLE)
             .addValues(
                 Query.Value.newBuilder()
                     .setType(Query.Type.UINT64)
                     .setValue(ByteString.copyFromUtf8("1"))
                     .build())
             .addValues(
                 Query.Value.newBuilder()
                     .setType(Query.Type.UINT64)
                     .setValue(ByteString.copyFromUtf8("2"))
                     .build())
             .addValues(
                 Query.Value.newBuilder()
                     .setType(Query.Type.UINT64)
                     .setValue(ByteString.copyFromUtf8("3"))
                     .build())
             .build()},
        // List of Float
        {Arrays.asList(1.2f, 3.4f, 5.6f), BindVariable.newBuilder()
                                              .setType(Query.Type.TUPLE)
                                              .addValues(
                                                  Query.Value.newBuilder()
                                                      .setType(Query.Type.FLOAT64)
                                                      .setValue(ByteString.copyFromUtf8("1.2"))
                                                      .build())
                                              .addValues(
                                                  Query.Value.newBuilder()
                                                      .setType(Query.Type.FLOAT64)
                                                      .setValue(ByteString.copyFromUtf8("3.4"))
                                                      .build())
                                              .addValues(
                                                  Query.Value.newBuilder()
                                                      .setType(Query.Type.FLOAT64)
                                                      .setValue(ByteString.copyFromUtf8("5.6"))
                                                      .build())
                                              .build()},
        {Arrays.asList(1.2, 3.4, 5.6), BindVariable.newBuilder()
                                           .setType(Query.Type.TUPLE)
                                           .addValues(
                                               Query.Value.newBuilder()
                                                   .setType(Query.Type.FLOAT64)
                                                   .setValue(ByteString.copyFromUtf8("1.2"))
                                                   .build())
                                           .addValues(
                                               Query.Value.newBuilder()
                                                   .setType(Query.Type.FLOAT64)
                                                   .setValue(ByteString.copyFromUtf8("3.4"))
                                                   .build())
                                           .addValues(
                                               Query.Value.newBuilder()
                                                   .setType(Query.Type.FLOAT64)
                                                   .setValue(ByteString.copyFromUtf8("5.6"))
                                                   .build())
                                           .build()}};
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
