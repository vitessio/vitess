/*
 * Copyright 2026 The Vitess Authors.

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

package io.client.grpc;

import com.google.protobuf.GeneratedMessage;

import io.vitess.proto.Vtgate.ExecuteRequest;

import java.lang.reflect.Method;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class GeneratedProtoCompatibilityTest {

  @Test
  public void generatedExecuteRequestUsesProtobufJavaRuntimeApi() {
    Assert.assertEquals(GeneratedMessage.class, ExecuteRequest.class.getSuperclass());

    for (Method method : ExecuteRequest.class.getDeclaredMethods()) {
      if (method.getName().equals("internalGetFieldAccessorTable")) {
        Assert.assertEquals(GeneratedMessage.FieldAccessorTable.class, method.getReturnType());
      }
    }
  }
}
