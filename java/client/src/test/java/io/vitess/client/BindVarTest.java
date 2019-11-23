/*
 * Copyright 2019 The Vitess Authors.

 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at

 *     http://www.apache.org/licenses/LICENSE-2.0

 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.vitess.client;

import static org.junit.Assert.assertEquals;

import com.google.common.primitives.UnsignedLong;
import com.google.protobuf.ByteString;

import io.vitess.proto.Query;
import io.vitess.proto.Query.BindVariable;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Arrays;
import java.util.Collection;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@RunWith(value = Parameterized.class)
public class BindVarTest {

  private Object input;
  private BindVariable expected;

  @Parameters
  public static Collection<Object[]> testParams() {
    Object[][] params = {
        // Already a BindVariable (e.g. in split query result)
        {BindVariable.newBuilder().setType(Query.Type.NULL_TYPE).build(),
            BindVariable.newBuilder().setType(Query.Type.NULL_TYPE).build()},
        // SQL NULL
        {null, BindVariable.newBuilder().setType(Query.Type.NULL_TYPE).build()},
        // String
        {"hello world",
            BindVariable.newBuilder().setType(Query.Type.VARCHAR)
                .setValue(ByteString.copyFromUtf8("hello world")).build()},
        // Bytes
        {new byte[]{1, 2, 3},
            BindVariable.newBuilder().setType(Query.Type.VARBINARY)
                .setValue(ByteString.copyFrom(new byte[]{1, 2, 3})).build()},
        // Int
        {123,
            BindVariable.newBuilder().setType(Query.Type.INT64)
                .setValue(ByteString.copyFromUtf8("123")).build()},
        {123L,
            BindVariable.newBuilder().setType(Query.Type.INT64)
                .setValue(ByteString.copyFromUtf8("123")).build()},
        {(short) 1,
            BindVariable.newBuilder().setType(Query.Type.INT64)
                .setValue(ByteString.copyFromUtf8("1")).build()},
        {(byte) 2,
            BindVariable.newBuilder().setType(Query.Type.INT64)
                .setValue(ByteString.copyFromUtf8("2")).build()},
        // Uint
        {UnsignedLong.fromLongBits(-1),
            BindVariable.newBuilder().setType(Query.Type.UINT64)
                .setValue(ByteString.copyFromUtf8("18446744073709551615")).build()},
        // Float
        {1.23f,
            BindVariable.newBuilder().setType(Query.Type.FLOAT64)
                .setValue(ByteString.copyFromUtf8("1.23")).build()},
        {1.23,
            BindVariable.newBuilder().setType(Query.Type.FLOAT64)
                .setValue(ByteString.copyFromUtf8("1.23")).build()},
        // List of Bytes
        {Arrays.asList(new byte[]{1, 2, 3}, new byte[]{4, 5, 6}),
            BindVariable.newBuilder().setType(Query.Type.TUPLE)
                .addValues(Query.Value.newBuilder().setType(Query.Type.VARBINARY)
                    .setValue(ByteString.copyFrom(new byte[]{1, 2, 3})).build())
                .addValues(Query.Value.newBuilder().setType(Query.Type.VARBINARY)
                    .setValue(ByteString.copyFrom(new byte[]{4, 5, 6})).build())
                .build()},
        // Boolean
        {true,
            BindVariable.newBuilder().setType(Query.Type.INT64)
                .setValue(ByteString.copyFromUtf8("1")).build()},
        {false,
            BindVariable.newBuilder().setType(Query.Type.INT64)
                .setValue(ByteString.copyFromUtf8("0")).build()},
        // BigDecimal: Check simple case. Note that BigDecimal.valueOf(0.23) is different from
        // new BigDecimal(0.23). The latter will create a number with large scale due to floating
        // point conversion artifacts.
        {BigDecimal.valueOf(0.23),
            BindVariable.newBuilder().setType(Query.Type.DECIMAL)
                .setValue(ByteString.copyFromUtf8("0.23")).build()},
        // BigDecimal: Check we don't mess with scale unnecessarily (don't add extra zeros).
        {new BigDecimal("123.123456789"),
            BindVariable.newBuilder().setType(Query.Type.DECIMAL)
                .setValue(ByteString.copyFromUtf8("123.123456789")).build()},
        // BigDecimal: Check round down when scale > 30 (MySQL max).
        {new BigDecimal("123.1999999999999999999999999999994"),
            BindVariable.newBuilder().setType(Query.Type.DECIMAL)
                .setValue(ByteString.copyFromUtf8("123.199999999999999999999999999999")).build()},
        // BigDecimal: Check round up when scale > 30 (MySQL max).
        {new BigDecimal("123.1999999999999999999999999999995"),
            BindVariable.newBuilder().setType(Query.Type.DECIMAL)
                .setValue(ByteString.copyFromUtf8("123.200000000000000000000000000000")).build()},
        // BigDecimal: Check that we DO NOT revert to scientific notation (e.g. "1.23E-10"),
        // because MySQL will interpret that as an *approximate* DOUBLE value, rather than
        // as an *exact* DECIMAL value:
        // http://dev.mysql.com/doc/refman/5.6/en/precision-math-numbers.html
        {new BigDecimal("0.000000000123456789123456789"),
            BindVariable.newBuilder().setType(Query.Type.DECIMAL)
                .setValue(ByteString.copyFromUtf8("0.000000000123456789123456789")).build()},
        {new BigInteger("123456789123456789"),
            BindVariable.newBuilder().setType(Query.Type.VARCHAR)
                .setValue(ByteString.copyFromUtf8("123456789123456789")).build()},
        // List of Int
        {Arrays.asList(1, 2, 3),
            BindVariable.newBuilder().setType(Query.Type.TUPLE)
                .addValues(Query.Value.newBuilder().setType(Query.Type.INT64)
                    .setValue(ByteString.copyFromUtf8("1")).build())
                .addValues(Query.Value.newBuilder().setType(Query.Type.INT64)
                    .setValue(ByteString.copyFromUtf8("2")).build())
                .addValues(Query.Value.newBuilder().setType(Query.Type.INT64)
                    .setValue(ByteString.copyFromUtf8("3")).build())
                .build()},
        {Arrays.asList(1L, 2L, 3L),
            BindVariable.newBuilder().setType(Query.Type.TUPLE)
                .addValues(Query.Value.newBuilder().setType(Query.Type.INT64)
                    .setValue(ByteString.copyFromUtf8("1")).build())
                .addValues(Query.Value.newBuilder().setType(Query.Type.INT64)
                    .setValue(ByteString.copyFromUtf8("2")).build())
                .addValues(Query.Value.newBuilder().setType(Query.Type.INT64)
                    .setValue(ByteString.copyFromUtf8("3")).build())
                .build()},
        // List of Uint
        {Arrays.asList(UnsignedLong.fromLongBits(1), UnsignedLong.fromLongBits(2),
            UnsignedLong.fromLongBits(3)),
            BindVariable.newBuilder().setType(Query.Type.TUPLE)
                .addValues(Query.Value.newBuilder().setType(Query.Type.UINT64)
                    .setValue(ByteString.copyFromUtf8("1")).build())
                .addValues(Query.Value.newBuilder().setType(Query.Type.UINT64)
                    .setValue(ByteString.copyFromUtf8("2")).build())
                .addValues(Query.Value.newBuilder().setType(Query.Type.UINT64)
                    .setValue(ByteString.copyFromUtf8("3")).build())
                .build()},
        // List of Float
        {Arrays.asList(1.2f, 3.4f, 5.6f),
            BindVariable.newBuilder().setType(Query.Type.TUPLE)
                .addValues(Query.Value.newBuilder().setType(Query.Type.FLOAT64)
                    .setValue(ByteString.copyFromUtf8("1.2")).build())
                .addValues(Query.Value.newBuilder().setType(Query.Type.FLOAT64)
                    .setValue(ByteString.copyFromUtf8("3.4")).build())
                .addValues(Query.Value.newBuilder().setType(Query.Type.FLOAT64)
                    .setValue(ByteString.copyFromUtf8("5.6")).build())
                .build()},
        {Arrays.asList(1.2, 3.4, 5.6),
            BindVariable.newBuilder().setType(Query.Type.TUPLE)
                .addValues(Query.Value.newBuilder().setType(Query.Type.FLOAT64)
                    .setValue(ByteString.copyFromUtf8("1.2")).build())
                .addValues(Query.Value.newBuilder().setType(Query.Type.FLOAT64)
                    .setValue(ByteString.copyFromUtf8("3.4")).build())
                .addValues(Query.Value.newBuilder().setType(Query.Type.FLOAT64)
                    .setValue(ByteString.copyFromUtf8("5.6")).build())
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
