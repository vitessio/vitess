/*
 * Copyright 2017 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
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
import io.vitess.proto.Vtgate.ExecuteEntityIdsRequest.EntityId;

import java.util.Arrays;
import java.util.Collection;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@RunWith(value = Parameterized.class)
public class EntityIdTest {

  private Object input;
  private EntityId expected;

  private static final ByteString KEYSPACE_ID = ByteString.copyFrom(new byte[]{1, 2, 3});

  @Parameters
  public static Collection<Object[]> testParams() {
    Object[][] params = {
        // SQL NULL
        {
            null,
            EntityId.newBuilder().setKeyspaceId(KEYSPACE_ID).setType(Query.Type.NULL_TYPE).build()
        },
        // String
        {
            "hello world",
            EntityId.newBuilder()
                .setKeyspaceId(KEYSPACE_ID)
                .setType(Query.Type.VARCHAR)
                .setValue(ByteString.copyFromUtf8("hello world"))
                .build()
        },
        // Bytes
        {
            new byte[]{1, 2, 3},
            EntityId.newBuilder()
                .setKeyspaceId(KEYSPACE_ID)
                .setType(Query.Type.VARBINARY)
                .setValue(ByteString.copyFrom(new byte[]{1, 2, 3}))
                .build()
        },
        // Int
        {
            123,
            EntityId.newBuilder()
                .setKeyspaceId(KEYSPACE_ID)
                .setType(Query.Type.INT64)
                .setValue(ByteString.copyFromUtf8("123"))
                .build()
        },
        {
            123L,
            EntityId.newBuilder()
                .setKeyspaceId(KEYSPACE_ID)
                .setType(Query.Type.INT64)
                .setValue(ByteString.copyFromUtf8("123"))
                .build()
        },
        // Uint
        {
            UnsignedLong.fromLongBits(-1),
            EntityId.newBuilder()
                .setKeyspaceId(KEYSPACE_ID)
                .setType(Query.Type.UINT64)
                .setValue(ByteString.copyFromUtf8("18446744073709551615"))
                .build()
        },
        // Float
        {
            1.23f,
            EntityId.newBuilder()
                .setKeyspaceId(KEYSPACE_ID)
                .setType(Query.Type.FLOAT64)
                .setValue(ByteString.copyFromUtf8("1.23"))
                .build()
        },
        {
            1.23,
            EntityId.newBuilder()
                .setKeyspaceId(KEYSPACE_ID)
                .setType(Query.Type.FLOAT64)
                .setValue(ByteString.copyFromUtf8("1.23"))
                .build()
        },
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
