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

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class ProtoTest {
  @Test
  public void testGetErrno() {
    final Map<String, Integer> testValues =
        new ImmutableMap.Builder<String, Integer>()
            .put("no errno", 0)
            .put("bad format (errno ", 0)
            .put("bad format (errno ...", 0)
            .put("good format, bad number (errno 123A)", 0)
            .put("good format, good number (errno 1234) ...", 1234)
            .build();

    for (Map.Entry<String, Integer> entry : testValues.entrySet()) {
      Assert.assertEquals((int) entry.getValue(), Proto.getErrno(entry.getKey()));
    }
  }

  @Test
  public void testGetSQLState() {
    final Map<String, String> testValues =
        new ImmutableMap.Builder<String, String>()
            .put("no sqlstate", "")
            .put("bad format (sqlstate ", "")
            .put("bad format (sqlstate ...", "")
            .put("good format (sqlstate abcd) ...", "abcd")
            .build();

    for (Map.Entry<String, String> entry : testValues.entrySet()) {
      Assert.assertEquals(entry.getValue(), Proto.getSQLState(entry.getKey()));
    }
  }
}
