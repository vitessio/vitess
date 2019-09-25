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

package io.vitess.util;

import com.google.protobuf.ByteString;

import io.vitess.proto.Query;
import io.vitess.proto.Topodata;

/**
 * Created by harshit.gangal on 25/01/16.
 */
public class Constants {

  public static final boolean JDBC_COMPLIANT = false;
  public static final String URL_PREFIX = "jdbc:vitess://";
  public static final String URL_PATTERN = "^jdbc:(vitess)://((\\w+)(:(\\w*))?@)?([^/?]*)(/"
      + "([^/?]*))?(/(\\w+))?(\\?(\\S+))?";
  public static final String VITESS_HOST = "Hostname of Vitess Server";
  public static final String VITESS_PORT = "Port number of Vitess Server";
  public static final String VITESS_DB_NAME = "Database name";
  public static final String VITESS_TABLET_TYPE = "Tablet Type to which Vitess will connect"
      + "(master, replica, rdonly)";
  public static final String DEFAULT_PORT = "15991";
  public static final Topodata.TabletType DEFAULT_TABLET_TYPE = Topodata.TabletType.MASTER;
  public static final String LITERAL_V = "v";
  public static final String LITERAL_SINGLE_QUOTE = "'";
  public static final int DRIVER_MAJOR_VERSION = 2;
  public static final int DRIVER_MINOR_VERSION = 2;
  public static final int MAX_BUFFER_SIZE = 65535;
  //Default Timeout in milliseconds
  public static final int DEFAULT_TIMEOUT = 30000;
  public static final String VITESS_KEYSPACE = "Keyspace name in Vitess Server";
  public static final Constants.QueryExecuteType DEFAULT_EXECUTE_TYPE = QueryExecuteType.SIMPLE;
  public static final String EXECUTE_TYPE_DESC = "Query execution type: simple or stream \n";
  public static final String USERNAME_DESC = "Username used for ACL validation \n";
  public static final Query.ExecuteOptions.IncludedFields DEFAULT_INCLUDED_FIELDS =
      Query.ExecuteOptions.IncludedFields.ALL;
  public static final String DEFAULT_KEYSPACE = "";
  public static final String DEFAULT_SHARD = "";
  public static final String DEFAULT_USERNAME = null;
  public static final String DEFAULT_TARGET = "";
  public static final String DEFAULT_CATALOG = DEFAULT_KEYSPACE;
  public static final ByteString ZERO_DATE_TIME_PREFIX = ByteString.copyFromUtf8("0000-00-00");

  private Constants() {
  }


  public static final class SQLExceptionMessages {

    public static final String CONN_UNAVAILABLE = "Connection not available";
    public static final String CONN_CLOSED = "Connection is Closed";
    public static final String INIT_FAILED = "Failed to Initialize Vitess JDBC Driver";
    public static final String INVALID_CONN_URL = "Connection URL is invalid";
    public static final String STMT_CLOSED = "Statement is closed";
    public static final String SQL_FEATURE_NOT_SUPPORTED = "SQL Feature Not Supported";
    public static final String TIMEOUT_NEGATIVE = "Timeout value cannot be negative";
    public static final String COMMIT_WHEN_AUTO_COMMIT_TRUE = "Cannot call commit when auto "
        + "commit is true";
    public static final String ROLLBACK_WHEN_AUTO_COMMIT_TRUE = "Cannot call rollback when auto "
        + "commit is true";
    public static final String CLOSED_RESULT_SET = "Result Set closed";
    public static final String INVALID_COLUMN_INDEX = "Invalid Column Index";
    public static final String VITESS_CURSOR_CLOSE_ERROR = "Getting Error while closing ResultSet";
    public static final String CONN_INIT_ERROR = "Connection initialization error";
    public static final String MALFORMED_URL = "Malformed URL Exception";
    public static final String SQL_TYPE_INFER = "Cannot infer the SQL type to use for an instance"
        + " of ";
    public static final String DML_NOT_ON_MASTER = "DML Statement cannot be executed on non "
        + "master instance type";
    public static final String SQL_EMPTY = "SQL statement is not valid";
    public static final String RESULT_SET_TYPE_NOT_SUPPORTED = "This Result Set type is not "
        + "supported";
    public static final String RESULT_SET_CONCUR_NOT_SUPPORTED = "This Result Set Concurrency is "
        + "not supported";
    public static final String METHOD_CALLED_ON_OPEN_TRANSACTION = "This method should not be "
        + "called when a transaction is open";
    public static final String ISOLATION_LEVEL_NOT_SUPPORTED = "This isolation level is not "
        + "supported";
    public static final String EXECUTOR_NULL = "Executor cannot be null";
    public static final String CLASS_CAST_EXCEPTION = "Unable to unwrap to ";
    public static final String INVALID_COLUMN_TYPE = "Invalid Column Type";
    public static final String UNKNOWN_COLUMN_TYPE = "Unknown Column Type";
    public static final String INVALID_RESULT_SET = "Unable to build ResultSet";
    public static final String METHOD_NOT_ALLOWED = "This method cannot be called using this "
        + "class object";
    public static final String SQL_RETURNED_RESULT_SET = "ResultSet generation is not allowed "
        + "through this method";
    public static final String ILLEGAL_VALUE_FOR = "Illegal value for ";
    public static final String METHOD_CALL_FAILED = "Failed to execute this method";
    public static final String CURSOR_NULL = "Cursor cannot be null";
    public static final String NO_COLUMN_ACCESSED = "No column was accessed before calling this "
        + "method";
    public static final String RESULT_SET_INIT_ERROR = "ResultSet initialization error";
    public static final String GENERATED_KEYS_NOT_REQUESTED =
        "Generated keys not requested. You need to specify "
            + "Statement.RETURN_GENERATED_KEYS to Statement.executeUpdate() or Connection"
            + ".prepareStatement()";
    public static final String NO_KEYSPACE = "Querying Database Information without providing "
        + "keyspace";
    public static final String QUERY_FAILED = "One or more queries failed in batch execution";
    public static final String READ_ONLY = "Connection has been set to read only and an update "
        + "was attempted";
    public static final String ZERO_TIMESTAMP = "Zero timestamp cannot be represented as java.sql"
        + ".Date";
  }


  public static final class Property {

    @Deprecated
    public static final String OLD_TABLET_TYPE = "TABLET_TYPE";
    public static final String TABLET_TYPE = "tabletType";
    public static final String HOST = "host";
    public static final String PORT = "port";
    public static final String DBNAME = "dbName";
    public static final String KEYSPACE = "keyspace";
    public static final String USERNAME = "userName";
    public static final String EXECUTE_TYPE = "executeType";
    public static final String TWOPC_ENABLED = "twopcEnabled";
    public static final String SHARD = "shard";

    public static final String USE_SSL = "useSSL";
    public static final String KEYSTORE = "keyStore";
    public static final String KEYSTORE_PASSWORD = "keyStorePassword";
    public static final String KEY_ALIAS = "keyAlias";
    public static final String KEY_PASSWORD = "keyPassword";
    public static final String TRUSTSTORE = "trustStore";
    public static final String TRUSTSTORE_PASSWORD = "trustStorePassword";
    public static final String TRUST_ALIAS = "trustAlias";

    public static final String KEYSTORE_FULL = "javax.net.ssl.keyStore";
    public static final String KEYSTORE_PASSWORD_FULL = "javax.net.ssl.keyStorePassword";
    public static final String KEY_ALIAS_FULL = "javax.net.ssl.keyAlias";
    public static final String KEY_PASSWORD_FULL = "javax.net.ssl.keyPassword";
    public static final String TRUSTSTORE_FULL = "javax.net.ssl.trustStore";
    public static final String TRUSTSTORE_PASSWORD_FULL = "javax.net.ssl.trustStorePassword";
    public static final String TRUST_ALIAS_FULL = "javax.net.ssl.trustAlias";
    public static final String INCLUDED_FIELDS = "includedFields";
    public static final String TARGET = "target";
  }


  public enum QueryExecuteType {
    SIMPLE, STREAM
  }

  public enum ZeroDateTimeBehavior {
    /**
     * This is the current behavior. It completely garbles null timestamps. It is mostly likely
     * entirely incorrect but we will keep it for backwards compatibility.
     */
    GARBLE,
    /**
     * This matches the MySQL JDBC driver convertToNull behavior.
     */
    CONVERTTONULL,
    /**
     * This matches the MySQL JDBC driver exception behavior.
     */
    EXCEPTION,
    /**
     * This matches the MySQL JDBC driver round behavior.
     */
    ROUND
  }
}
