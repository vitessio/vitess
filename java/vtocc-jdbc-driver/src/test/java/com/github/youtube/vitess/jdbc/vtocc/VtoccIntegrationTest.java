package com.github.youtube.vitess.jdbc.vtocc;

import com.google.common.collect.Maps;
import com.google.common.io.CharStreams;
import com.google.common.io.Files;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonPrimitive;

import com.github.youtube.vitess.jdbc.bson.Driver;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.lang.ProcessBuilder.Redirect;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@RunWith(JUnit4.class)
public class VtoccIntegrationTest {

  public static final String VTOCC_PORT = System.getProperty("vtocc.port");
  public static final String VTROOT = Files.simplifyPath(System.getProperty("VTROOT"));
  public static final String VTDATAROOT = Files.simplifyPath(System.getProperty("VTDATAROOT"));

  private static final String TARGET_TESTS_OUTPUT_DIRECTORY =
      VtoccIntegrationTest.class.getResource("/").getPath();
  private static Process vtoccProcess;

  @BeforeClass
  public static void setupJdbcDriver() throws Exception {
    DriverManager.registerDriver(new Driver());
  }

  @BeforeClass
  public static void setupVtoccServer() throws IOException, InterruptedException {
    ProcessBuilder pb = new ProcessBuilder(VTROOT + "/bin/vtocc",
        "-port", VTOCC_PORT,
        "-alsologtostderr",
        "-log_dir", VTDATAROOT + "/tmp",
        "-db-config-app-charset", "utf8",
        "-db-config-app-dbname", "vt_test_keyspace",
        "-db-config-app-host", "localhost",
        "-db-config-app-unixsocket", VTDATAROOT + "/mysql.sock",
        "-db-config-app-uname", "vt_dba",
        "-db-config-app-keyspace", "test_keyspace",
        "-db-config-app-shard", "0");
    pb.redirectOutput(Redirect.INHERIT);
    pb.redirectError(Redirect.INHERIT);
    vtoccProcess = pb.start();
    Thread.sleep(TimeUnit.SECONDS.toMillis(10));
  }

  @AfterClass
  public static void teardownVtoccServer() {
    vtoccProcess.destroy();
  }


  @Test
  public void testIntegrationTests() throws Exception {
    try (Connection connection = DriverManager.getConnection(
        "jdbc:vtocc://localhost:" + VTOCC_PORT + "/test_keyspace/0")) {

      VtoccIntegrationSqlCasesTester tester = new VtoccIntegrationSqlCasesTester(
          connection);

      tester.initializeDatabaseState();
      tester.verifyDatabaseState();
    }
  }

  /**
   * Reuses tests from python implementation of Vtocc.
   *
   * This allows us to use always up to date test suite of immense size with ease.
   *
   * Parses and executes tests generated in {@code sql_tests.json} and {@code test_schema.sql}.
   * Original files are in {@code $VTTOP/test/test_data/test_schema.sql} and in {@code
   * $VTTOP/test/queryservice_tests/nocache_cases.py}.
   */
  public static class VtoccIntegrationSqlCasesTester {

    public static final String TEST_SCHEMA_SQL = "test_schema.sql";
    public static final String SQL_TESTS_JSON = "sql_tests.json";
    private static final Logger logger = LoggerFactory
        .getLogger(VtoccIntegrationSqlCasesTester.class);
    private final Connection connection;

    public VtoccIntegrationSqlCasesTester(Connection connection) {
      this.connection = connection;
    }

    /**
     * Reads and executes {@code testdata/test_shcema.sql} on a provided connection.
     */
    public void initializeDatabaseState()
        throws SQLException, IOException {
      // workaround to support mysql driver which initializes in autocommit=true state
      connection.setAutoCommit(false);

      File schemaFile = getFile(TEST_SCHEMA_SQL);
      logger.info("Initializing database: " + schemaFile.getPath());
      List<String> sqlStatements = CharStreams.readLines(
          Files.newReader(schemaFile, StandardCharsets.UTF_8));
      for (String sql : sqlStatements) {
        executeSql(connection, sql);
      }
    }

    /**
     * Reads and executes tests in {@code testdata/sql_tests.json} on a provided connection.
     *
     * See {@link #verifySqlTestsArray(Connection, JsonArray)} for format description.
     */
    public void verifyDatabaseState()
        throws SQLException, IOException {
      // workaround to support mysql driver which initializes in autocommit=true state
      connection.setAutoCommit(false);

      File sqlTestsFile = getFile(SQL_TESTS_JSON);
      logger.info("Verifying database: " + sqlTestsFile.getPath());
      JsonArray testsArray = new JsonParser()
          .parse(Files.newReader(sqlTestsFile, StandardCharsets.UTF_8)).getAsJsonArray();
      verifySqlTestsArray(connection, testsArray);

      // verify that tests are failing/passing at least sometimes
      verifySqlTest(connection, new JsonParser().parse(
          "{'sql':'select id from vtocc_b limit 1', 'result':[[1]]}").getAsJsonObject());
      boolean testPassed = false;
      try {
        verifySqlTest(connection, new JsonParser().parse(
            "{'sql':'select id from vtocc_b limit 1', 'result':[[42]]}").getAsJsonObject());
        testPassed = true;
      } catch (AssertionError ignored) {
        // exception is expected to be thrown
      } finally {
        Assert.assertFalse(testPassed);
      }
    }

    /**
     * Helper to load data file from the same package.
     */
    private File getFile(String fileName) {
      return new File(TARGET_TESTS_OUTPUT_DIRECTORY + "/" + fileName);
    }

    /**
     * Verifies all the tests in array provided.
     *
     * JSON formats supported: Normal test, just like in {@link #verifySqlTest(Connection,
     * JsonObject)} or {@code "sqls_and_cases"} which is an array of normal tests.
     * <pre>
     * {
     *   "doc": "bind values",
     *   "sqls_and_cases": [
     *       "begin",
     *       {
     *           "sql": ...
     *           ...
     *       },
     *       "commit",
     *       ...
     * }
     * </pre>
     */
    private void verifySqlTestsArray(Connection connection, JsonArray testsArray)
        throws SQLException {
      for (JsonElement jsonElement : testsArray) {
        JsonObject testObject = jsonElement.getAsJsonObject();
        if (testObject.has("sql")) {
          verifySqlTest(connection, testObject);
        } else if (testObject.has("sqls_and_cases")) {
          JsonArray sqlsAndCasesArray = testObject.getAsJsonArray("sqls_and_cases");
          for (JsonElement sqlAndCaseElement : sqlsAndCasesArray) {
            if (sqlAndCaseElement.isJsonObject()) {
              verifySqlTest(connection, sqlAndCaseElement.getAsJsonObject());
            } else if (sqlAndCaseElement.isJsonPrimitive()
                && sqlAndCaseElement.getAsJsonPrimitive().isString()) {
              executeSql(connection, sqlAndCaseElement.getAsString());
            } else {
              throw new IllegalArgumentException("Can not parse subtest: " + sqlAndCaseElement);
            }
          }
        } else {
          throw new IllegalArgumentException("Can not parse type of test: " + testObject);
        }
      }
    }

    /**
     * Runs specified SQL statement against database and checks its result.
     *
     * JSON format supported:
     * <pre>
     * {
     *   "sql": "select eid, id from vtocc_a union select eid, id from vtocc_b",
     *   "bindings": {
     *       "a": 1
     *   },
     *   "result": [
     *       [
     *           1,
     *           1
     *       ],
     *       [
     *           1,
     *           2
     *       ]
     *   ],
     *   ...
     * }
     * </pre>
     */
    private void verifySqlTest(Connection connection, JsonObject testObject)
        throws SQLException {
      String sql = testObject.get("sql").getAsString();
      logger.debug("Testing: {}", sql);
      switch (sql) {
        case "commit":
          connection.commit();
          return;
        case "begin":
          // we're always in a transaction
          return;
        case "rollback":
          throw new UnsupportedOperationException("Rollback is not supported in tests");
      }

      // Our driver only supports '?' as a param placeholder, all params in tests are named
      Map<String, Integer> bindingIndexes = Maps.newHashMap();
      Matcher bindingMatcher = Pattern.compile(":(\\w+)").matcher(sql);
      int bindingIndex = 0;
      while (bindingMatcher.find()) {
        bindingIndex++;
        bindingIndexes.put(bindingMatcher.group(1), bindingIndex);
      }
      bindingIndexes.put("v1", 1);
      bindingIndexes.put("v2", 2);
      bindingIndexes.put("v3", 3);
      bindingIndexes.put("v4", 4);
      bindingIndexes.put("v5", 5);
      sql = sql.replaceAll(":(\\w+)", "?");
      PreparedStatement preparedStatement = connection.prepareStatement(sql);

      // Bind parameters as an indexed params, not named ones
      if ((testObject.has("bindings") && testObject.get("bindings").isJsonObject())) {
        JsonObject bindingsObject = testObject.getAsJsonObject("bindings");
        for (Entry<String, JsonElement> keyValueElement : bindingsObject.entrySet()) {
          Integer parameterIndex = bindingIndexes.get(keyValueElement.getKey());
          Assert.assertNotNull(
              "Can not find variable binding for " + keyValueElement.getKey(), parameterIndex);
          Object value = parseJsonValue(keyValueElement.getValue());
          preparedStatement.setObject(parameterIndex, value);
          bindingIndex++;
        }
      }

      try {
        if (!testObject.has("result") || testObject.get("result").isJsonNull()) {
          // This is DML or INSERT statement
          preparedStatement.executeUpdate();
          return;
        }

        ResultSet resultSet = preparedStatement.executeQuery();

        // And verify that result is the one specified in the test
        JsonArray resultArrayArray = testObject.getAsJsonArray("result");
        int rowIndex = 0;
        for (JsonElement resultArrayElement : resultArrayArray) {
          rowIndex++;
          Assert.assertTrue("Not enough results, row: " + rowIndex, resultSet.next());
          JsonArray resultArray = resultArrayElement.getAsJsonArray();
          int columnIndex = 0;
          for (JsonElement resultElement : resultArray) {
            columnIndex++;
            Object expected = parseJsonValue(resultElement);
            Object actual = resultSet.getObject(columnIndex);
            try {
              if (expected instanceof Number && actual instanceof Number) {
                // Special case to compare all types of Numbers
                Assert.assertEquals(expected.toString(), actual.toString());
              } else if (expected instanceof String && actual instanceof byte[]) {
                // Byte arrays are specified as strings in a JSON
                Assert.assertEquals(expected, new String((byte[]) actual, StandardCharsets.UTF_8));
                Assert.assertArrayEquals((byte[]) actual, resultSet.getBytes(columnIndex));
              } else if (expected instanceof Number && actual instanceof Date) {
                // Dates are specified as only year, sometimes
                Assert.assertEquals(Date.valueOf(expected.toString() + "-01-01"), actual);
                Assert.assertEquals(actual, resultSet.getDate(columnIndex));
              } else if (expected instanceof Boolean && actual instanceof String) {
                // Driver returns Strings instead of Booleans for some reason
                Assert.assertEquals(expected, ((String) actual).charAt(0) != '0');
                Assert.assertEquals(expected, resultSet.getBoolean(columnIndex));
              } else {
                Assert.assertEquals(expected, actual);
              }
            } catch (AssertionError e) {
              throw new AssertionError("Expected JSON: " + resultElement
                  + "\nExpected value: " + expected + " (" + expected.getClass().getSimpleName()
                  + ")"
                  + "\nActual: '" + actual + " (" + actual.getClass().getSimpleName() + ")"
                  + "\nRow/Column: " + rowIndex + "/" + columnIndex, e);
            }
          }
          Assert.assertEquals(columnIndex, resultSet.getMetaData().getColumnCount());
        }
        Assert.assertFalse("Too many results", resultSet.next());
      } catch (AssertionError | RuntimeException | SQLException e) {
        throw new AssertionError("Test failed: " + testObject, e);
      }
    }

    /**
     * Executes SQL including transaction language: begin, commit and rollback.
     */
    private void executeSql(Connection connection, String sql) throws SQLException {
      logger.debug("Executing: {}", sql);
      switch (sql) {
        case "commit":
          connection.commit();
          return;
        case "begin":
          // we're always in a transaction
          return;
        case "rollback":
          throw new UnsupportedOperationException("Rollback is not supported in tests");
      }
      connection.prepareStatement(sql).execute();
    }

    /**
     * Parse expected value out of JSON. Turns out that Vtocc test cases contain peculiar values
     * that should be treated as {@link java.sql.Timestamp} as an example. Therefore no standard
     * library is good enough for us.
     *
     * Supported value formats:
     * <pre>
     * {'decimal':'1.00'} - BigDecimal
     * {'date':'2012-01-01'} - java.sql.Date
     * {'datetime':'2012-01-01 00:00'} - java.sql.Timestamp
     * {'timedelta':'00:00'} - java.sql.Time
     * 99 - Byte
     * 9999 - Short
     * 999999999 - Integer
     * 999999999999999999 - Long
     * 9999999999999999999... - BigDecimal
     * 1.0 - Double
     * "\u0001" - True
     * "\u0000" - False
     * "..." - String
     * </pre>
     */
    private Object parseJsonValue(JsonElement element) {
      if (element.isJsonObject()) {
        JsonObject object = element.getAsJsonObject();
        try {
          if (object.has("decimal")) {
            return new BigDecimal(object.get("decimal").getAsString());
          } else if (object.has("date")) {
            return Date.valueOf(object.get("date").getAsString());
          } else if (object.has("datetime")) {
            return Timestamp.valueOf(object.get("datetime").getAsString());
          } else if (object.has("timedelta")) {
            return Time.valueOf(object.get("timedelta").getAsString());
          } else {
            throw new IllegalArgumentException("Can not parse value object: " + object);
          }
        } catch (RuntimeException e) {
          throw new IllegalArgumentException("Can not parse value object: " + object, e);
        }
      }
      if (!element.isJsonPrimitive()) {
        throw new IllegalArgumentException("Can not parse value: " + element);
      }
      JsonPrimitive primitive = element.getAsJsonPrimitive();
      String resultString = primitive.getAsString();
      if (primitive.isNumber() && !resultString.contains(".")) {
        if (resultString.length() <= 2) {
          return primitive.getAsByte();
        } else if (resultString.length() <= 4) {
          return primitive.getAsShort();
        } else if (resultString.length() <= 9) {
          return primitive.getAsInt();
        } else if (resultString.length() <= 18) {
          return primitive.getAsLong();
        } else {
          // BigInteger is not supported by PreparedStatement
          return primitive.getAsBigDecimal();
        }
      } else if (primitive.isNumber() && resultString.contains(".")) {
        return primitive.getAsDouble();
      } else if (primitive.isString() && resultString.equals("\u0000")) {
        return false;
      } else if (primitive.isString() && resultString.equals("\u0001")) {
        return true;
      } else if (primitive.isString()) {
        return resultString;
      } else {
        throw new IllegalArgumentException("Can not parse value primitive: " + primitive);
      }
    }
  }

}
