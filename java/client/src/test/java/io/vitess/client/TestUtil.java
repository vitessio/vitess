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

import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import com.google.gson.reflect.TypeToken;

import io.vitess.proto.Query;
import io.vitess.proto.Topodata.TabletType;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.joda.time.Duration;
import org.junit.Assert;
import vttest.Vttest.VTTestTopology;

public class TestUtil {

  static final Logger logger = LogManager.getLogger(TestUtil.class.getName());
  public static final String PROPERTY_KEY_CLIENT_TEST_ENV = "vitess.client.testEnv";
  public static final String PROPERTY_KEY_CLIENT_TEST_PORT = "vitess.client.testEnv.portName";
  public static final String PROPERTY_KEY_CLIENT_FACTORY_CLASS = "vitess.client.factory";

  /**
   * Setup MySQL, Vttablet and VtGate instances required for the tests. This uses the py/vttest
   * framework to start and stop instances.
   */
  public static void setupTestEnv(TestEnv testEnv) throws Exception {
    List<String> command = testEnv.getSetupCommand(15000);
    logger.info("setup command: " + command.toString());
    ProcessBuilder pb = new ProcessBuilder(command);
    pb.redirectErrorStream(true);
    Process p = pb.start();
    BufferedReader br = new BufferedReader(new InputStreamReader(p.getInputStream()));

    // Read the vtgate port from stdout as JSON with a "port" field.
    String line;
    while ((line = br.readLine()) != null) {
      logger.info("run_local_database: " + line);
      if (!line.startsWith("{")) {
        continue;
      }
      try {
        Type mapType = new TypeToken<Map<String, Object>>() {
        }.getType();
        Map<String, Object> map = new Gson().fromJson(line, mapType);
        testEnv.setPythonScriptProcess(p);
        testEnv.setPort(
            ((Double) map.get(System.getProperty(PROPERTY_KEY_CLIENT_TEST_PORT))).intValue());
        return;
      } catch (JsonSyntaxException e) {
        logger.error("JsonSyntaxException parsing setup command output: " + line, e);
      }
    }
    Assert.fail("setup script failed to parse vtgate port");
  }

  /**
   * Teardown the test instances, if any.
   */
  public static void teardownTestEnv(TestEnv testEnv) throws Exception {
    Process process = testEnv.getPythonScriptProcess();
    if (process != null) {
      logger.info("sending empty line to run_local_database to stop test setup");
      process.getOutputStream().write("\n".getBytes());
      process.getOutputStream().flush();
      process.waitFor();
      testEnv.setPythonScriptProcess(null);
    }
    testEnv.clearTestOutput();
  }

  public static TestEnv getTestEnv(String keyspace, VTTestTopology topology) {
    String testEnvClass = System.getProperty(PROPERTY_KEY_CLIENT_TEST_ENV);
    try {
      Class<?> clazz = Class.forName(testEnvClass);
      TestEnv env = (TestEnv) clazz.newInstance();
      env.setKeyspace(keyspace);
      env.setTopology(topology);
      return env;
    } catch (ClassNotFoundException | IllegalAccessException | InstantiationException e) {
      throw new RuntimeException(e);
    }
  }

  public static RpcClientFactory getRpcClientFactory() {
    String rpcClientFactoryClass = System.getProperty(PROPERTY_KEY_CLIENT_FACTORY_CLASS);
    try {
      Class<?> clazz = Class.forName(rpcClientFactoryClass);
      return (RpcClientFactory) clazz.newInstance();
    } catch (ClassNotFoundException | IllegalAccessException | InstantiationException e) {
      throw new RuntimeException(e);
    }
  }

  public static VTGateBlockingConn getBlockingConn(TestEnv testEnv) {
    // Dial timeout
    Context ctx = Context.getDefault().withDeadlineAfter(Duration.millis(5000));
    return new VTGateBlockingConn(
        getRpcClientFactory().create(ctx, "localhost:" + testEnv.getPort()),
        testEnv.getKeyspace());
  }

  public static void insertRows(TestEnv testEnv, int startId, int count) throws Exception {
    try (VTGateBlockingConn conn = getBlockingConn(testEnv)) {
      // Deadline for the overall insert loop
      Context ctx = Context.getDefault().withDeadlineAfter(Duration.millis(5000));

      VTGateBlockingTx tx = conn.begin(ctx);
      String insertSql = "insert into vtgate_test "
          + "(id, name, age, percent) values (:id, :name, :age, :percent)";
      Map<String, Object> bindVars = new HashMap<>();
      for (int id = startId; id - startId < count; id++) {
        bindVars.put("id", id);
        bindVars.put("name", "name_" + id);
        bindVars.put("age", id % 10);
        bindVars.put("percent", id / 100.0);
        tx.execute(ctx, insertSql, bindVars, TabletType.MASTER,
            Query.ExecuteOptions.IncludedFields.ALL);
      }
      tx.commit(ctx);
    }
  }
}
