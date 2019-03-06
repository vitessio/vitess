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

package io.vitess.jdbc;

import java.sql.SQLException;
import java.util.Properties;

import org.junit.Assert;
import org.junit.BeforeClass;

public class BaseTest {

  String dbURL = "jdbc:vitess://locahost:9000/vt_keyspace/keyspace";

  @BeforeClass
  public static void setUp() {
    try {
      Class.forName("io.vitess.jdbc.VitessDriver");
    } catch (ClassNotFoundException e) {
      Assert.fail("Driver is not in the CLASSPATH -> " + e.getMessage());
    }
  }

  protected VitessConnection getVitessConnection() throws SQLException {
    return new VitessConnection(dbURL, new Properties());
  }

  protected VitessStatement getVitessStatement() throws SQLException {
    return new VitessStatement(getVitessConnection());
  }
}
