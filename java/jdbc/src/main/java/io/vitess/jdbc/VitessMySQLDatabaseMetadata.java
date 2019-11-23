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

package io.vitess.jdbc;

import com.google.common.annotations.VisibleForTesting;

import io.vitess.proto.Query;
import io.vitess.util.Constants;
import io.vitess.util.MysqlDefs;

import org.apache.commons.lang.StringUtils;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.RowIdLifetime;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.SortedMap;
import java.util.StringTokenizer;
import java.util.TreeMap;
import java.util.logging.Logger;

/**
 * Created by ashudeep.sharma on 15/02/16.
 */
public class VitessMySQLDatabaseMetadata extends VitessDatabaseMetaData implements
    DatabaseMetaData {

  private static final String DRIVER_NAME = "Vitess MySQL JDBC Driver";
  private static Logger logger = Logger.getLogger(VitessMySQLDatabaseMetadata.class.getName());
  private static String mysqlKeywordsThatArentSQL92;

  static {

    String[] allMySQLKeywords = new String[]{"ACCESSIBLE", "ADD", "ALL", "ALTER", "ANALYZE", "AND",
        "AS", "ASC", "ASENSITIVE", "BEFORE", "BETWEEN", "BIGINT", "BINARY", "BLOB", "BOTH", "BY",
        "CALL", "CASCADE", "CASE", "CHANGE", "CHAR", "CHARACTER", "CHECK", "COLLATE", "COLUMN",
        "CONDITION", "CONNECTION", "CONSTRAINT", "CONTINUE", "CONVERT", "CREATE", "CROSS",
        "CURRENT_DATE", "CURRENT_TIME", "CURRENT_TIMESTAMP", "CURRENT_USER", "CURSOR", "DATABASE",
        "DATABASES", "DAY_HOUR", "DAY_MICROSECOND", "DAY_MINUTE", "DAY_SECOND", "DEC", "DECIMAL",
        "DECLARE", "DEFAULT", "DELAYED", "DELETE", "DESC", "DESCRIBE", "DETERMINISTIC", "DISTINCT",
        "DISTINCTROW", "DIV", "DOUBLE", "DROP", "DUAL", "EACH", "ELSE", "ELSEIF", "ENCLOSED",
        "ESCAPED", "EXISTS", "EXIT", "EXPLAIN", "FALSE", "FETCH", "FLOAT", "FLOAT4", "FLOAT8",
        "FOR", "FORCE", "FOREIGN", "FROM", "FULLTEXT", "GRANT", "GROUP", "HAVING", "HIGH_PRIORITY",
        "HOUR_MICROSECOND", "HOUR_MINUTE", "HOUR_SECOND", "IF", "IGNORE", "IN", "INDEX", "INFILE",
        "INNER", "INOUT", "INSENSITIVE", "INSERT", "INT", "INT1", "INT2", "INT3", "INT4", "INT8",
        "INTEGER", "INTERVAL", "INTO", "IS", "ITERATE", "JOIN", "KEY", "KEYS", "KILL", "LEADING",
        "LEAVE", "LEFT", "LIKE", "LIMIT", "LINEAR", "LINES", "LOAD", "LOCALTIME", "LOCALTIMESTAMP",
        "LOCK", "LONG", "LONGBLOB", "LONGTEXT", "LOOP", "LOW_PRIORITY", "MATCH", "MEDIUMBLOB",
        "MEDIUMINT", "MEDIUMTEXT", "MIDDLEINT", "MINUTE_MICROSECOND", "MINUTE_SECOND", "MOD",
        "MODIFIES", "NATURAL", "NOT", "NO_WRITE_TO_BINLOG", "NULL", "NUMERIC", "ON", "OPTIMIZE",
        "OPTION", "OPTIONALLY", "OR", "ORDER", "OUT", "OUTER", "OUTFILE", "PRECISION", "PRIMARY",
        "PROCEDURE", "PURGE", "RANGE", "READ", "READS", "READ_ONLY", "READ_WRITE", "REAL",
        "REFERENCES", "REGEXP", "RELEASE", "RENAME", "REPEAT", "REPLACE", "REQUIRE", "RESTRICT",
        "RETURN", "REVOKE", "RIGHT", "RLIKE", "SCHEMA", "SCHEMAS", "SECOND_MICROSECOND", "SELECT",
        "SENSITIVE", "SEPARATOR", "SET", "SHOW", "SMALLINT", "SPATIAL", "SPECIFIC", "SQL",
        "SQLEXCEPTION", "SQLSTATE", "SQLWARNING", "SQL_BIG_RESULT", "SQL_CALC_FOUND_ROWS",
        "SQL_SMALL_RESULT", "SSL", "STARTING", "STRAIGHT_JOIN", "TABLE", "TERMINATED", "THEN",
        "TINYBLOB", "TINYINT", "TINYTEXT", "TO", "TRAILING", "TRIGGER", "TRUE", "UNDO", "UNION",
        "UNIQUE", "UNLOCK", "UNSIGNED", "UPDATE", "USAGE", "USE", "USING", "UTC_DATE", "UTC_TIME",
        "UTC_TIMESTAMP", "VALUES", "VARBINARY", "VARCHAR", "VARCHARACTER", "VARYING", "WHEN",
        "WHERE", "WHILE", "WITH", "WRITE", "X509", "XOR", "YEAR_MONTH", "ZEROFILL"};
    String[] sql92Keywords = new String[]{"ABSOLUTE", "EXEC", "OVERLAPS", "ACTION", "EXECUTE",
        "PAD", "ADA", "EXISTS", "PARTIAL", "ADD", "EXTERNAL", "PASCAL", "ALL", "EXTRACT",
        "POSITION", "ALLOCATE", "FALSE", "PRECISION", "ALTER", "FETCH", "PREPARE", "AND", "FIRST",
        "PRESERVE", "ANY", "FLOAT", "PRIMARY", "ARE", "FOR", "PRIOR", "AS", "FOREIGN", "PRIVILEGES",
        "ASC", "FORTRAN", "PROCEDURE", "ASSERTION", "FOUND", "PUBLIC", "AT", "FROM", "READ",
        "AUTHORIZATION", "FULL", "REAL", "AVG", "GET", "REFERENCES", "BEGIN", "GLOBAL", "RELATIVE",
        "BETWEEN", "GO", "RESTRICT", "BIT", "GOTO", "REVOKE", "BIT_LENGTH", "GRANT", "RIGHT",
        "BOTH", "GROUP", "ROLLBACK", "BY", "HAVING", "ROWS", "CASCADE", "HOUR", "SCHEMA",
        "CASCADED", "IDENTITY", "SCROLL", "CASE", "IMMEDIATE", "SECOND", "CAST", "IN", "SECTION",
        "CATALOG", "INCLUDE", "SELECT", "CHAR", "INDEX", "SESSION", "CHAR_LENGTH", "INDICATOR",
        "SESSION_USER", "CHARACTER", "INITIALLY", "SET", "CHARACTER_LENGTH", "INNER", "SIZE",
        "CHECK", "INPUT", "SMALLINT", "CLOSE", "INSENSITIVE", "SOME", "COALESCE", "INSERT", "SPACE",
        "COLLATE", "INT", "SQL", "COLLATION", "INTEGER", "SQLCA", "COLUMN", "INTERSECT", "SQLCODE",
        "COMMIT", "INTERVAL", "SQLERROR", "CONNECT", "INTO", "SQLSTATE", "CONNECTION", "IS",
        "SQLWARNING", "CONSTRAINT", "ISOLATION", "SUBSTRING", "CONSTRAINTS", "JOIN", "SUM",
        "CONTINUE", "KEY", "SYSTEM_USER", "CONVERT", "LANGUAGE", "TABLE", "CORRESPONDING", "LAST",
        "TEMPORARY", "COUNT", "LEADING", "THEN", "CREATE", "LEFT", "TIME", "CROSS", "LEVEL",
        "TIMESTAMP", "CURRENT", "LIKE", "TIMEZONE_HOUR", "CURRENT_DATE", "LOCAL", "TIMEZONE_MINUTE",
        "CURRENT_TIME", "LOWER", "TO", "CURRENT_TIMESTAMP", "MATCH", "TRAILING", "CURRENT_USER",
        "MAX", "TRANSACTION", "CURSOR", "MIN", "TRANSLATE", "DATE", "MINUTE", "TRANSLATION", "DAY",
        "MODULE", "TRIM", "DEALLOCATE", "MONTH", "TRUE", "DEC", "NAMES", "UNION", "DECIMAL",
        "NATIONAL", "UNIQUE", "DECLARE", "NATURAL", "UNKNOWN", "DEFAULT", "NCHAR", "UPDATE",
        "DEFERRABLE", "NEXT", "UPPER", "DEFERRED", "NO", "USAGE", "DELETE", "NONE", "USER", "DESC",
        "NOT", "USING", "DESCRIBE", "NULL", "VALUE", "DESCRIPTOR", "NULLIF", "VALUES",
        "DIAGNOSTICS", "NUMERIC", "VARCHAR", "DISCONNECT", "OCTET_LENGTH", "VARYING", "DISTINCT",
        "OF", "VIEW", "DOMAIN", "ON", "WHEN", "DOUBLE", "ONLY", "WHENEVER", "DROP", "OPEN", "WHERE",
        "ELSE", "OPTION", "WITH", "END", "OR", "WORK", "END-EXEC", "ORDER", "WRITE", "ESCAPE",
        "OUTER", "YEAR", "EXCEPT", "OUTPUT", "ZONE", "EXCEPTION"};
    TreeMap mySqlKeywordMap = new TreeMap();

    for (String allMySQLKeyword : allMySQLKeywords) {
      mySqlKeywordMap.put(allMySQLKeyword, null);
    }

    HashMap sql92KeywordMap = new HashMap(sql92Keywords.length);

    for (String sql92Keyword : sql92Keywords) {
      sql92KeywordMap.put(sql92Keyword, null);
    }

    Iterator sql92KeywordIterator = sql92KeywordMap.keySet().iterator();

    while (sql92KeywordIterator.hasNext()) {
      mySqlKeywordMap.remove(sql92KeywordIterator.next());
    }

    StringBuffer keywordBuf = new StringBuffer();
    sql92KeywordIterator = mySqlKeywordMap.keySet().iterator();
    if (sql92KeywordIterator.hasNext()) {
      keywordBuf.append(sql92KeywordIterator.next().toString());
    }

    while (sql92KeywordIterator.hasNext()) {
      keywordBuf.append(",");
      keywordBuf.append(sql92KeywordIterator.next().toString());
    }

    mysqlKeywordsThatArentSQL92 = keywordBuf.toString();
  }

  private int maxBufferSize = '\uffff';

  public VitessMySQLDatabaseMetadata(VitessConnection connection) throws SQLException {
    this.setConnection(connection);
  }

  public boolean nullsAreSortedAtStart() throws SQLException {
    return false;
  }

  public boolean nullsAreSortedAtEnd() throws SQLException {
    return false;
  }

  public String getDriverName() throws SQLException {
    return DRIVER_NAME;
  }

  public String getSQLKeywords() throws SQLException {
    return mysqlKeywordsThatArentSQL92;
  }

  public String getSystemFunctions() throws SQLException {
    return super.getSystemFunctions() + ",PASSWORD,ENCRYPT";
  }

  public boolean supportsConvert() throws SQLException {
    throw new SQLFeatureNotSupportedException(
        Constants.SQLExceptionMessages.SQL_FEATURE_NOT_SUPPORTED);
  }

  public boolean supportsConvert(int fromType, int toType) throws SQLException {
    return false;
  }

  public boolean supportsTableCorrelationNames() throws SQLException {
    return false;
  }

  public boolean supportsDifferentTableCorrelationNames() throws SQLException {
    return false;
  }

  public boolean supportsANSI92EntryLevelSQL() throws SQLException {
    return false;
  }

  public boolean supportsANSI92IntermediateSQL() throws SQLException {
    return false;
  }

  public boolean supportsANSI92FullSQL() throws SQLException {
    return false;
  }

  public boolean supportsIntegrityEnhancementFacility() throws SQLException {
    return false;
  }

  public boolean supportsSchemasInDataManipulation() throws SQLException {
    return false;
  }

  public boolean supportsSchemasInProcedureCalls() throws SQLException {
    return false;
  }

  public boolean supportsSchemasInTableDefinitions() throws SQLException {
    return false;
  }

  public boolean supportsSchemasInPrivilegeDefinitions() throws SQLException {
    return false;
  }

  public boolean supportsCatalogsInDataManipulation() throws SQLException {
    return false;
  }

  public boolean supportsCatalogsInProcedureCalls() throws SQLException {
    return false;
  }

  public boolean supportsCatalogsInTableDefinitions() throws SQLException {
    return false;
  }

  public boolean supportsCatalogsInIndexDefinitions() throws SQLException {
    return false;
  }

  public boolean supportsCatalogsInPrivilegeDefinitions() throws SQLException {
    return false;
  }

  public boolean supportsPositionedDelete() throws SQLException {
    return false;
  }

  public boolean supportsPositionedUpdate() throws SQLException {
    return false;
  }

  public int getMaxColumnsInTable() throws SQLException {
    return 0;
  }

  public int getMaxCursorNameLength() throws SQLException {
    return 64;
  }

  public int getMaxSchemaNameLength() throws SQLException {
    return 0;
  }

  public int getMaxCatalogNameLength() throws SQLException {
    return 32;
  }

  public int getMaxRowSize() throws SQLException {
    return 2147483639;
  }

  public boolean doesMaxRowSizeIncludeBlobs() throws SQLException {
    return false;
  }

  public int getMaxStatementLength() throws SQLException {
    return 65531;
  }

  public int getDefaultTransactionIsolation() throws SQLException {
    return this.connection.getDbProperties().getIsolationLevel();
  }

  public boolean supportsTransactions() throws SQLException {
    return true;
  }

  public boolean supportsTransactionIsolationLevel(int level) throws SQLException {
    switch (level) {
      case Connection.TRANSACTION_READ_COMMITTED:
      case Connection.TRANSACTION_READ_UNCOMMITTED:
      case Connection.TRANSACTION_REPEATABLE_READ:
      case Connection.TRANSACTION_SERIALIZABLE:
        return true;

      default:
        return false;
    }
  }

  @SuppressWarnings("StringBufferReplaceableByString")
  public ResultSet getTables(String catalog, String schemaPattern, String tableNamePattern,
      String[] types) throws SQLException {
    ResultSet resultSet = null;
    VitessStatement vitessStatement = null;
    boolean reportTables = false;
    boolean reportViews = false;
    boolean reportSystemTables = false;
    boolean reportSystemViews = false;
    boolean reportLocalTemporaries = false;
    final SortedMap<TableMetaDataKey, ArrayList<String>> sortedRows = new TreeMap<>();

    if (null == tableNamePattern) {
      tableNamePattern = "%";
    }
    if (null == catalog || catalog.length() == 0) {
      catalog = this.connection.getCatalog();
    }
    ArrayList<ArrayList<String>> data = new ArrayList<>();
    try {
      vitessStatement = new VitessStatement(this.connection);
      resultSet = vitessStatement.executeQuery(
          "SHOW FULL TABLES FROM " + this.quotedId + catalog + this.quotedId + " LIKE \'"
              + tableNamePattern + "\'");

      if (null == types || types.length == 0) {
        reportTables = reportViews = reportSystemTables = reportSystemViews =
            reportLocalTemporaries = true;
      } else {
        for (String type : types) {
          if (TableType.TABLE.equalsTo(type)) {
            reportTables = true;

          } else if (TableType.VIEW.equalsTo(type)) {
            reportViews = true;

          } else if (TableType.SYSTEM_TABLE.equalsTo(type)) {
            reportSystemTables = true;

          } else if (TableType.SYSTEM_VIEW.equalsTo(type)) {
            reportSystemViews = true;

          } else if (TableType.LOCAL_TEMPORARY.equalsTo(type)) {
            reportLocalTemporaries = true;
          }
        }
      }

      int typeColumnIndex = 0;
      boolean hasTableTypes;
      try {
        typeColumnIndex = resultSet.findColumn("table_type");
        hasTableTypes = true;
      } catch (SQLException sqlEx) {
        try {
          typeColumnIndex = resultSet.findColumn("Type");
          hasTableTypes = true;
        } catch (SQLException sqlEx2) {
          hasTableTypes = false;
        }
      }

      while (resultSet.next()) {
        ArrayList<String> row = new ArrayList<>();
        row.add(0, catalog);
        row.add(1, null);
        row.add(2, resultSet.getString(1));
        row.add(3, "");
        row.add(4, null);
        row.add(5, null);
        row.add(6, null);
        row.add(7, null);
        row.add(8, null);
        row.add(9, null);

        if (hasTableTypes) {
          String tableType = resultSet.getString(typeColumnIndex);
          switch (TableType.getTableTypeCompliantWith(tableType)) {
            case TABLE:
              boolean reportTable = false;
              TableMetaDataKey tablesKey = null;
              if (reportSystemTables) {
                row.add(3, TableType.TABLE.toString());
                tablesKey = new TableMetaDataKey(TableType.TABLE.getName(), catalog, null,
                    resultSet.getString(1));
                reportTable = true;
              }
              if (reportTable) {
                sortedRows.put(tablesKey, row);
              }
              break;

            case VIEW:
              if (reportViews) {
                row.add(3, TableType.VIEW.toString());
                sortedRows.put(new TableMetaDataKey(TableType.VIEW.getName(), catalog, null,
                    resultSet.getString(1)), row);
              }
              break;

            case SYSTEM_TABLE:
              if (reportSystemTables) {
                row.add(3, TableType.SYSTEM_TABLE.toString());
                sortedRows.put(new TableMetaDataKey(TableType.SYSTEM_TABLE.getName(), catalog, null,
                    resultSet.getString(1)), row);
              }
              break;

            case SYSTEM_VIEW:
              if (reportSystemViews) {
                row.add(3, TableType.SYSTEM_VIEW.toString());
                sortedRows.put(new TableMetaDataKey(TableType.SYSTEM_VIEW.getName(), catalog, null,
                    resultSet.getString(1)), row);
              }
              break;

            case LOCAL_TEMPORARY:
              if (reportLocalTemporaries) {
                row.add(3, TableType.LOCAL_TEMPORARY.toString());
                sortedRows.put(
                    new TableMetaDataKey(TableType.LOCAL_TEMPORARY.getName(), catalog, null,
                        resultSet.getString(1)), row);
              }
              break;

            default:
              row.add(3, TableType.TABLE.toString());
              sortedRows.put(new TableMetaDataKey(TableType.TABLE.getName(), catalog, null,
                  resultSet.getString(1)), row);
              break;
          }
        } else {
          if (reportTables) {
            // Pre-MySQL-5.0.1, tables only
            row.add(3, TableType.TABLE.toString());
            sortedRows.put(new TableMetaDataKey(TableType.TABLE.getName(), catalog, null,
                resultSet.getString(1)), row);
          }
        }
        data.add(row);
      }
    } finally {
      if (null != resultSet) {
        resultSet.close();
      }
      if (null != vitessStatement) {
        vitessStatement.close();
      }
    }
    String[] columnNames = new String[]{"TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "TABLE_TYPE",
        "REMARKS", "TYPE_CAT", "TYPE_SCHEM", "TYPE_NAME", "SELF_REFERENCING_COL_NAME",
        "REF_GENERATION"};
    Query.Type[] columnTypes = new Query.Type[]{Query.Type.VARCHAR, Query.Type.VARCHAR,
        Query.Type.VARCHAR, Query.Type.VARCHAR, Query.Type.VARCHAR, Query.Type.VARCHAR,
        Query.Type.VARCHAR, Query.Type.VARCHAR, Query.Type.VARCHAR, Query.Type.VARCHAR};

    return new VitessResultSet(columnNames, columnTypes, data, this.connection);
  }

  public ResultSet getSchemas() throws SQLException {
    String[] columnNames = {"TABLE_SCHEM", "TABLE_CATALOG"};
    Query.Type[] columnType = {Query.Type.CHAR, Query.Type.CHAR};
    return new VitessResultSet(columnNames, columnType, new String[][]{}, this.connection);
  }

  public ResultSet getCatalogs() throws SQLException {
    ResultSet resultSet;
    VitessStatement vitessStatement;
    String getCatalogQB = "SHOW DATABASES";

    vitessStatement = new VitessStatement(this.connection);
    resultSet = vitessStatement.executeQuery(getCatalogQB);

    ArrayList<String> row = new ArrayList<>();
    ArrayList<ArrayList<String>> data = new ArrayList<>();
    while (resultSet.next()) {
      row.add(resultSet.getString(1));
    }
    Collections.sort(row);

    for (String result : row) {
      ArrayList<String> resultAsList = new ArrayList<>();
      resultAsList.add(result);
      data.add(resultAsList);
    }
    resultSet.close();
    vitessStatement.close();
    String[] columnName = new String[]{"TABLE_CAT"};
    Query.Type[] columntype = new Query.Type[]{Query.Type.CHAR};
    return new VitessResultSet(columnName, columntype, data, this.connection);
  }

  public ResultSet getTableTypes() throws SQLException {
    String[] columnNames = {"table_type"};
    Query.Type[] columnType = {Query.Type.VARCHAR};
    String[][] data = new String[][]{{"LOCAL TEMPORARY"}, {"SYSTEM TABLES"}, {"SYSTEM VIEW"},
        {"TABLE"}, {"VIEW"}};
    return new VitessResultSet(columnNames, columnType, data, this.connection);
  }

  @SuppressWarnings("StringBufferReplaceableByString")
  public ResultSet getColumns(String catalog, String schemaPattern, String tableNamePattern,
      String columnNamePattern) throws SQLException {
    ResultSet resultSet = null;
    VitessStatement vitessStatement = new VitessStatement(this.connection);
    ArrayList<ArrayList<String>> data = new ArrayList<>();
    //Null Matches All
    if (null == columnNamePattern) {
      columnNamePattern = "%";
    }
    if (null == catalog || catalog.length() == 0) {
      catalog = this.connection.getCatalog();
    }
    try {
      ArrayList<String> tableList = new ArrayList<>();
      ResultSet tables = null;
      if (null == tableNamePattern) {
        try {
          tables = getTables(catalog, schemaPattern, "%", new String[0]);
          while (tables.next()) {
            String tableName = tables.getString("TABLE_NAME");
            tableList.add(tableName);
          }
        } finally {
          if (null != tables) {
            tables.close();
          }
        }
      } else {
        try {
          tables = getTables(catalog, schemaPattern, tableNamePattern, new String[0]);
          while (tables.next()) {
            String tableName = tables.getString("TABLE_NAME");
            tableList.add(tableName);
          }
        } finally {
          if (null != tables) {
            tables.close();
          }
        }
      }
      for (String tableName : tableList) {
        resultSet = null;
        try {

          // Return correct ordinals if column name pattern is not '%'
          // Currently, MySQL doesn't show enough data to do this, so we do it the 'hard' way...Once
          // _SYSTEM tables are in, this should be
          // much easier
          boolean fixUpOrdinalsRequired = false;
          Map<String, Integer> ordinalFixUpMap = null;
          if (!columnNamePattern.equals("%")) {
            fixUpOrdinalsRequired = true;
            vitessStatement = new VitessStatement(this.connection);
            resultSet = vitessStatement.executeQuery(
                "SHOW FULL COLUMNS FROM " + this.quotedId + tableName + this.quotedId + " FROM "
                    + this.quotedId + catalog + this.quotedId);
            ordinalFixUpMap = new HashMap<>();

            int fullOrdinalPos = 1;
            while (resultSet.next()) {
              String fullOrdColName = resultSet.getString("Field");
              ordinalFixUpMap.put(fullOrdColName, fullOrdinalPos++);
            }
          }
          resultSet = vitessStatement.executeQuery(
              "SHOW FULL COLUMNS FROM " + this.quotedId + tableName + this.quotedId + " FROM "
                  + this.quotedId + catalog + this.quotedId + " LIKE "
                  + Constants.LITERAL_SINGLE_QUOTE + columnNamePattern
                  + Constants.LITERAL_SINGLE_QUOTE);
          int ordPos = 1;

          while (resultSet.next()) {
            ArrayList<String> row = new ArrayList<>();
            row.add(0, catalog);
            row.add(1, null);
            row.add(2, tableName);
            row.add(3, resultSet.getString("Field"));
            TypeDescriptor typeDesc = new TypeDescriptor(resultSet.getString("Type"),
                resultSet.getString("Null"));

            row.add(4, Short.toString(typeDesc.dataType));

            // DATA_TYPE (jdbc)
            row.add(5, typeDesc.typeName); // TYPE_NAME
            // (native)
            if (null == typeDesc.columnSize) {
              row.add(6, null);
            } else {
              String collation = resultSet.getString("Collation");
              int mbminlen = 1;
              if (collation != null && ("TEXT".equals(typeDesc.typeName) || "TINYTEXT"
                  .equals(typeDesc.typeName) || "MEDIUMTEXT".equals(typeDesc.typeName))) {
                if (collation.indexOf("ucs2") > -1 || collation.indexOf("utf16") > -1) {
                  mbminlen = 2;
                } else if (collation.indexOf("utf32") > -1) {
                  mbminlen = 4;
                }
              }
              row.add(6, mbminlen == 1 ? typeDesc.columnSize.toString()
                  : Integer.toString(typeDesc.columnSize / mbminlen));
            }
            row.add(7, Integer.toString(typeDesc.bufferLength));
            row.add(8, typeDesc.decimalDigits == null ? null : typeDesc.decimalDigits.toString());
            row.add(9, Integer.toString(typeDesc.numPrecRadix));
            row.add(10, Integer.toString(typeDesc.nullability));

            //
            // Doesn't always have this field, depending on version
            //
            //
            // REMARK column
            //
            row.add(11, "Comment");

            // COLUMN_DEF
            row.add(12,
                resultSet.getString("Default") == null ? null : resultSet.getString("Default"));

            row.add(13, Integer.toString(0));// SQL_DATA_TYPE
            row.add(14, Integer.toString(0));// SQL_DATE_TIME_SUB

            if (StringUtils.indexOfIgnoreCase(typeDesc.typeName, "CHAR") != -1
                || StringUtils.indexOfIgnoreCase(typeDesc.typeName, "BLOB") != -1
                || StringUtils.indexOfIgnoreCase(typeDesc.typeName, "TEXT") != -1
                || StringUtils.indexOfIgnoreCase(typeDesc.typeName, "BINARY") != -1) {
              row.add(15, row.get(6)); // CHAR_OCTET_LENGTH
            } else {
              row.add(15, Integer.toString(0));
            }

            // ORDINAL_POSITION
            if (!fixUpOrdinalsRequired) {
              row.add(16, Integer.toString(ordPos++));
            } else {
              String origColName = resultSet.getString("Field");
              Integer realOrdinal = ordinalFixUpMap.get(origColName);

              if (realOrdinal != null) {
                row.add(16, realOrdinal.toString());
              } else {
                throw new SQLException(
                    "Can not find column in full column list to determine true ordinal position.");
              }
            }

            row.add(17, typeDesc.isNullable);

            // We don't support REF or DISTINCT types
            row.add(18, null);
            row.add(19, null);
            row.add(20, null);
            row.add(21, null);
            String extra = resultSet.getString("Extra");
            if (null != extra) {
              row.add(22,
                  StringUtils.indexOfIgnoreCase(extra, "auto_increment") != -1 ? "YES" : "NO");
              row.add(23, StringUtils.indexOfIgnoreCase(extra, "generated") != -1 ? "YES" : "NO");
            }
            data.add(row);
          }
        } finally {
          if (null != resultSet) {
            resultSet.close();
          }
        }
      }
    } finally {
      if (null != resultSet) {
        resultSet.close();
      }
      vitessStatement.close();
    }

    String[] columnNames = new String[]{"TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "COLUMN_NAME",
        "DATA_TYPE", "TYPE_NAME", "COLUMN_SIZE", "BUFFER_LENGTH", "DECIMAL_DIGITS",
        "NUM_PREC_RADIX", "NULLABLE", "REMARKS", "COLUMN_DEF", "SQL_DATA_TYPE", "SQL_DATETIME_SUB",
        "CHAR_OCTET_LENGTH", "ORDINAL_POSITION", "IS_NULLABLE", "SCOPE_CATALOG", "SCOPE_SCHEMA",
        "SCOPE_TABLE", "SOURCE_DATA_TYPE", "IS_AUTOINCREMENT", "IS_GENERATEDCOLUMN"};

    Query.Type[] columnType = new Query.Type[]{Query.Type.CHAR, Query.Type.CHAR, Query.Type.CHAR,
        Query.Type.CHAR, Query.Type.INT32, Query.Type.CHAR, Query.Type.INT32, Query.Type.INT32,
        Query.Type.INT32, Query.Type.INT32, Query.Type.INT32, Query.Type.CHAR, Query.Type.CHAR,
        Query.Type.INT32, Query.Type.INT32, Query.Type.INT32, Query.Type.INT32, Query.Type.CHAR,
        Query.Type.CHAR, Query.Type.CHAR, Query.Type.CHAR, Query.Type.INT16, Query.Type.CHAR,
        Query.Type.CHAR};

    return new VitessResultSet(columnNames, columnType, data, this.connection);
  }

  public ResultSet getColumnPrivileges(String catalog, String schema, String table,
      String columnNamePattern) throws SQLException {
    throw new SQLFeatureNotSupportedException(
        Constants.SQLExceptionMessages.SQL_FEATURE_NOT_SUPPORTED);
  }

  public ResultSet getTablePrivileges(String catalog, String schemaPattern, String tableNamePattern)
      throws SQLException {
    throw new SQLFeatureNotSupportedException(
        Constants.SQLExceptionMessages.SQL_FEATURE_NOT_SUPPORTED);
  }

  @SuppressWarnings("StringBufferReplaceableByString")
  public ResultSet getBestRowIdentifier(String catalog, String schema, String table, int scope,
      boolean nullable) throws SQLException {
    ResultSet resultSet = null;
    VitessStatement vitessStatement = new VitessStatement(this.connection);

    if (null == table) {
      throw new SQLException("Table Parameter cannot be null in getBestRowIdentifier");
    }

    String[] columnName = new String[]{"SCOPE", "COLUMN_NAME", "DATA_TYPE", "TYPE_NAME",
        "COLUMN_SIZE", "BUFFER_LENGTH", "DECIMAL_DIGITS", "PSEUDO_COLUMN"};

    Query.Type[] columnType = new Query.Type[]{Query.Type.INT16, Query.Type.CHAR, Query.Type.INT32,
        Query.Type.CHAR, Query.Type.INT32, Query.Type.INT32, Query.Type.INT16, Query.Type.INT16};

    ArrayList<ArrayList<String>> data = new ArrayList<>();

    try {
      resultSet = vitessStatement.executeQuery(
          "SHOW COLUMNS FROM " + this.quotedId + table + this.quotedId + "" + " FROM "
              + this.quotedId + catalog + this.quotedId);

      while (resultSet.next()) {
        ArrayList<String> row = new ArrayList<>();
        String keyType = resultSet.getString("Key");
        if (keyType != null && StringUtils.startsWithIgnoreCase(keyType, "PRI")) {
          row.add(Integer.toString(DatabaseMetaData.bestRowSession));
          row.add(resultSet.getString("Field"));
          String type = resultSet.getString("Type");
          int size = this.maxBufferSize;
          int decimals = 0;
          if (type.indexOf("enum") != -1) {
            String temp = type.substring(type.indexOf("("), type.indexOf(")"));
            StringTokenizer tokenizer = new StringTokenizer(temp, ",");

            int maxLength;
            for (maxLength = 0; tokenizer.hasMoreTokens();
                maxLength = Math.max(maxLength, tokenizer.nextToken().length() - 2)) {
              ;
            }

            size = maxLength;
            decimals = 0;
            type = "enum";
          } else if (type.indexOf("(") != -1) {
            if (type.indexOf(",") != -1) {
              size = Integer.parseInt(type.substring(type.indexOf("(") + 1, type.indexOf(",")));
              decimals = Integer.parseInt(type.substring(type.indexOf(",") + 1, type.indexOf(")")));
            } else {
              size = Integer.parseInt(type.substring(type.indexOf("(") + 1, type.indexOf(")")));
            }

            type = type.substring(0, type.indexOf("("));
          }

          row.add(Integer.toString(MysqlDefs.mysqlToJavaType(type)));
          row.add(type);
          row.add(Integer.toString(size + decimals));
          row.add(Integer.toString(size + decimals));
          row.add(Integer.toString(decimals));
          row.add(Integer.toString(DatabaseMetaData.bestRowNotPseudo));
          data.add(row);
        }
      }
    } finally {
      if (resultSet != null) {
        resultSet.close();
      }
      vitessStatement.close();
    }
    return new VitessResultSet(columnName, columnType, data, this.connection);
  }

  public ResultSet getVersionColumns(String catalog, String schema, String table)
      throws SQLException {
    if (null == table) {
      throw new SQLException("Table cannot be null");
    }
    ResultSet resultSet = null;
    VitessStatement vitessStatement = null;
    ArrayList<ArrayList<String>> data = new ArrayList<>();

    StringBuilder getVersionColumnsQB = new StringBuilder();
    getVersionColumnsQB.append("SHOW COLUMNS FROM ");
    getVersionColumnsQB.append(this.quotedId);
    getVersionColumnsQB.append(table);
    getVersionColumnsQB.append(this.quotedId);
    getVersionColumnsQB.append(" FROM ");
    getVersionColumnsQB.append(this.quotedId);
    getVersionColumnsQB.append(catalog);
    getVersionColumnsQB.append(this.quotedId);
    getVersionColumnsQB.append(" WHERE Extra LIKE '%on update CURRENT_TIMESTAMP%'");

    try {
      vitessStatement = new VitessStatement(this.connection);
      resultSet = vitessStatement.executeQuery(getVersionColumnsQB.toString());
      ArrayList<String> row;
      while (resultSet.next()) {
        row = new ArrayList<>();
        TypeDescriptor typeDesc = new TypeDescriptor(resultSet.getString("Type"),
            resultSet.getString("Null"));
        row.add(0, null);
        row.add(1, resultSet.getString("Field"));
        row.add(2, Short.toString(typeDesc.dataType));
        row.add(3, typeDesc.typeName);
        row.add(4, typeDesc.columnSize == null ? null : typeDesc.columnSize.toString());
        row.add(5, Integer.toString(typeDesc.bufferLength));
        row.add(6, typeDesc.decimalDigits == null ? null : typeDesc.decimalDigits.toString());
        row.add(7, Integer.toString(java.sql.DatabaseMetaData.versionColumnNotPseudo));
        data.add(row);
      }
    } finally {
      if (null != resultSet) {
        resultSet.close();
        vitessStatement.close();
      }
    }
    String[] columnNames = new String[]{"SCOPE", "COLUMN_NAME", "DATA_TYPE", "TYPE_NAME",
        "COLUMN_SIZE", "BUFFER_LENGTH", "DECIMAL_DIGITS", "PSEUDO_COLUMN"};

    Query.Type[] columnType = new Query.Type[]{Query.Type.INT16, Query.Type.CHAR, Query.Type.INT32,
        Query.Type.CHAR, Query.Type.INT32, Query.Type.INT32, Query.Type.INT16, Query.Type.INT16};
    return new VitessResultSet(columnNames, columnType, data, this.connection);
  }

  @SuppressWarnings("StringBufferReplaceableByString")
  public ResultSet getPrimaryKeys(String catalog, String schema, String table) throws SQLException {

    if (null == table) {
      throw new SQLException("Table Name Cannot be Null");
    }
    ResultSet resultSet = null;
    VitessStatement vitessStatement = new VitessStatement(this.connection);
    ArrayList<ArrayList<String>> sortedData = new ArrayList<>();
    try {

      resultSet = vitessStatement.executeQuery(
          "SHOW KEYS FROM " + this.quotedId + table + this.quotedId + " " + "FROM " + this.quotedId
              + catalog + this.quotedId);

      TreeMap<String, ArrayList<String>> sortMap = new TreeMap<>();

      while (resultSet.next()) {
        String keyType = resultSet.getString("Key_name");
        ArrayList<String> row = new ArrayList<>();
        if (null != keyType) {
          if (keyType.equalsIgnoreCase("PRIMARY") || keyType.equalsIgnoreCase("PRI")) {
            row.add(0, (catalog == null) ? "" : catalog);
            row.add(1, null);
            row.add(2, table);

            String columnName = resultSet.getString("Column_name");
            row.add(3, columnName);
            row.add(4, resultSet.getString("Seq_in_index"));
            row.add(5, keyType);
            sortMap.put(columnName, row);
          }
        }
      }

      // Now pull out in column name sorted order
      for (ArrayList<String> row : sortMap.values()) {
        sortedData.add(row);
      }
    } finally {
      if (null != resultSet) {
        resultSet.close();
      }
      vitessStatement.close();
    }

    String[] columnNames = new String[]{"TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "COLUMN_NAME",
        "KEY_SEQ", "PK_NAME"};
    Query.Type[] columnType = new Query.Type[]{Query.Type.CHAR, Query.Type.CHAR, Query.Type.CHAR,
        Query.Type.CHAR, Query.Type.INT16, Query.Type.CHAR};

    return new VitessResultSet(columnNames, columnType, sortedData, this.connection);
  }

  public ResultSet getImportedKeys(String catalog, String schema, String table)
      throws SQLException {
    if (null == table) {
      throw new SQLException("Table Name Cannot be Null");
    }
    ResultSet resultSet = null;
    VitessStatement vitessStatement = new VitessStatement(this.connection);
    ArrayList<ArrayList<String>> rows = new ArrayList<>();
    try {
      resultSet = vitessStatement
          .executeQuery("SHOW CREATE TABLE " + this.quotedId + table + this.quotedId);
      while (resultSet.next()) {
        extractForeignKeyForTable(rows, resultSet.getString(2), catalog, table);
      }
    } finally {
      if (resultSet != null) {
        resultSet.close();
      }
    }
    String[] columnNames = new String[]{"PKTABLE_CAT", "PKTABLE_SCHEM", "PKTABLE_NAME",
        "PKCOLUMN_NAME", "FKTABLE_CAT", "FKTABLE_SCHEM", "FKTABLE_NAME", "FKCOLUMN_NAME", "KEY_SEQ",
        "UPDATE_RULE", "DELETE_RULE", "FK_NAME", "PK_NAME", "DEFERRABILITY"};
    Query.Type[] columnType = new Query.Type[]{Query.Type.CHAR, Query.Type.CHAR, Query.Type.CHAR,
        Query.Type.CHAR, Query.Type.CHAR, Query.Type.CHAR, Query.Type.CHAR, Query.Type.CHAR,
        Query.Type.INT16, Query.Type.INT16, Query.Type.INT16, Query.Type.CHAR, Query.Type.CHAR,
        Query.Type.INT16};
    return new VitessResultSet(columnNames, columnType, rows, this.connection);
  }

  @VisibleForTesting
  void extractForeignKeyForTable(List<ArrayList<String>> rows, String createTableString,
      String catalog, String table) throws SQLException {
    StringTokenizer lineTokenizer = new StringTokenizer(createTableString, "\n");

    while (lineTokenizer.hasMoreTokens()) {
      String line = lineTokenizer.nextToken().trim();
      String constraintName = null;
      if (StringUtils.startsWithIgnoreCase(line, "CONSTRAINT")) {
        boolean usingBackTicks = true;
        int beginPos = io.vitess.util.StringUtils.indexOfQuoteDoubleAware(line, this.quotedId, 0);
        if (beginPos == -1) {
          beginPos = line.indexOf("\"");
          usingBackTicks = false;
        }
        if (beginPos != -1) {
          int endPos;
          if (usingBackTicks) {
            endPos = io.vitess.util.StringUtils
                .indexOfQuoteDoubleAware(line, this.quotedId, beginPos + 1);
          } else {
            endPos = io.vitess.util.StringUtils.indexOfQuoteDoubleAware(line, "\"", beginPos + 1);
          }
          if (endPos != -1) {
            constraintName = line.substring(beginPos + 1, endPos);
            line = line.substring(endPos + 1, line.length()).trim();
          }
        }
      }

      if (line.startsWith("FOREIGN KEY")) {
        if (line.endsWith(",")) {
          line = line.substring(0, line.length() - 1);
        }
        int indexOfFK = line.indexOf("FOREIGN KEY");
        String localColumnName = null;
        String referencedCatalogName = io.vitess.util.StringUtils
            .quoteIdentifier(catalog, this.quotedId);
        String referencedTableName = null;
        String referencedColumnName = null;
        if (indexOfFK != -1) {
          int afterFk = indexOfFK + "FOREIGN KEY".length();

          int indexOfRef = io.vitess.util.StringUtils
              .indexOfIgnoreCase(afterFk, line, "REFERENCES", this.quotedId, this.quotedId);
          if (indexOfRef != -1) {
            int indexOfParenOpen = line.indexOf('(', afterFk);
            int indexOfParenClose = io.vitess.util.StringUtils
                .indexOfIgnoreCase(indexOfParenOpen, line, ")", this.quotedId, this.quotedId);
            localColumnName = line.substring(indexOfParenOpen + 1, indexOfParenClose);

            int afterRef = indexOfRef + "REFERENCES".length();
            int referencedColumnBegin = io.vitess.util.StringUtils
                .indexOfIgnoreCase(afterRef, line, "(", this.quotedId, this.quotedId);

            if (referencedColumnBegin != -1) {
              referencedTableName = line.substring(afterRef, referencedColumnBegin);
              int referencedColumnEnd = io.vitess.util.StringUtils
                  .indexOfIgnoreCase(referencedColumnBegin + 1, line, ")", this.quotedId,
                      this.quotedId);
              if (referencedColumnEnd != -1) {
                referencedColumnName = line
                    .substring(referencedColumnBegin + 1, referencedColumnEnd);
              }
              int indexOfCatalogSep = io.vitess.util.StringUtils
                  .indexOfIgnoreCase(0, referencedTableName, ".", this.quotedId, this.quotedId);
              if (indexOfCatalogSep != -1) {
                referencedCatalogName = referencedTableName.substring(0, indexOfCatalogSep);
                referencedTableName = referencedTableName.substring(indexOfCatalogSep + 1);
              }
            }
          }
        }
        if (constraintName == null) {
          constraintName = "not_available";
        }
        List<String> localColumnsList = io.vitess.util.StringUtils
            .split(localColumnName, ",", this.quotedId, this.quotedId);
        List<String> referColumnsList = io.vitess.util.StringUtils
            .split(referencedColumnName, ",", this.quotedId, this.quotedId);
        if (localColumnsList.size() != referColumnsList.size()) {
          throw new SQLException(
              "Mismatch columns list for foreign key local and reference columns");
        }
        // Report a separate row for each column in the foreign key. All values the same except
        // the column name.
        for (int i = 0; i < localColumnsList.size(); i++) {
          String localColumn = localColumnsList.get(i);
          String referColumn = referColumnsList.get(i);
          ArrayList<String> row = new ArrayList<>(14);
          row.add(io.vitess.util.StringUtils
              .unQuoteIdentifier(referencedCatalogName, this.quotedId)); // PKTABLE_CAT
          row.add(null); // PKTABLE_SCHEM
          row.add(io.vitess.util.StringUtils
              .unQuoteIdentifier(referencedTableName, this.quotedId)); // PKTABLE_NAME
          row.add(io.vitess.util.StringUtils
              .unQuoteIdentifier(referColumn, this.quotedId)); // PKCOLUMN_NAME
          row.add(catalog); // FKTABLE_CAT
          row.add(null); // FKTABLE_SCHEM
          row.add(table); // FKTABLE_NAME
          row.add(io.vitess.util.StringUtils
              .unQuoteIdentifier(localColumn, this.quotedId)); // FKCOLUMN_NAME
          row.add(Integer.toString(i + 1)); // KEY_SEQ
          int[] actions = getForeignKeyActions(line);
          row.add(Integer.toString(actions[1])); // UPDATE_RULE
          row.add(Integer.toString(actions[0])); // DELETE_RULE
          row.add(constraintName); // FK_NAME
          row.add(null); // PK_NAME
          row.add(Integer
              .toString(java.sql.DatabaseMetaData.importedKeyNotDeferrable)); // DEFERRABILITY
          rows.add(row);
        }
      }
    }
  }


  /**
   * Parses the constraint to see what actions are taking for update and delete, such as cascade.
   *
   * @param constraint the constraint to parse
   * @return the code from {@link DatabaseMetaData} corresponding to the foreign actions for the
   *     constraint
   */
  private int[] getForeignKeyActions(String constraint) {
    int[] actions = new int[]{java.sql.DatabaseMetaData.importedKeyNoAction,
        java.sql.DatabaseMetaData.importedKeyNoAction};
    int lastParenIndex = constraint.lastIndexOf(")");
    if (lastParenIndex != (constraint.length() - 1)) {
      String cascadeOptions = constraint.substring(lastParenIndex + 1).trim()
          .toUpperCase(Locale.ENGLISH);
      actions[0] = getCascadeDeleteOption(cascadeOptions);
      actions[1] = getCascadeUpdateOption(cascadeOptions);
    }
    return actions;
  }

  /**
   * Parses the cascade option string and returns the DBMD constant that represents it (for
   * deletes)
   *
   * @param cascadeOptions the comment from 'SHOW TABLE STATUS'
   * @return the DBMD constant that represents the cascade option
   */
  private int getCascadeDeleteOption(String cascadeOptions) {
    int onDeletePos = cascadeOptions.indexOf("ON DELETE");
    if (onDeletePos != -1) {
      String deleteOptions = cascadeOptions.substring(onDeletePos, cascadeOptions.length());
      if (deleteOptions.startsWith("ON DELETE CASCADE")) {
        return java.sql.DatabaseMetaData.importedKeyCascade;
      } else if (deleteOptions.startsWith("ON DELETE SET NULL")) {
        return java.sql.DatabaseMetaData.importedKeySetNull;
      } else if (deleteOptions.startsWith("ON DELETE RESTRICT")) {
        return java.sql.DatabaseMetaData.importedKeyRestrict;
      } else if (deleteOptions.startsWith("ON DELETE NO ACTION")) {
        return java.sql.DatabaseMetaData.importedKeyNoAction;
      }
    }
    return java.sql.DatabaseMetaData.importedKeyNoAction;
  }

  /**
   * Parses the cascade option string and returns the DBMD constant that represents it (for
   * Updates)
   *
   * @param cascadeOptions the comment from 'SHOW TABLE STATUS'
   * @return the DBMD constant that represents the cascade option
   */
  private int getCascadeUpdateOption(String cascadeOptions) {
    int onUpdatePos = cascadeOptions.indexOf("ON UPDATE");
    if (onUpdatePos != -1) {
      String updateOptions = cascadeOptions.substring(onUpdatePos, cascadeOptions.length());
      if (updateOptions.startsWith("ON UPDATE CASCADE")) {
        return java.sql.DatabaseMetaData.importedKeyCascade;
      } else if (updateOptions.startsWith("ON UPDATE SET NULL")) {
        return java.sql.DatabaseMetaData.importedKeySetNull;
      } else if (updateOptions.startsWith("ON UPDATE RESTRICT")) {
        return java.sql.DatabaseMetaData.importedKeyRestrict;
      } else if (updateOptions.startsWith("ON UPDATE NO ACTION")) {
        return java.sql.DatabaseMetaData.importedKeyNoAction;
      }
    }
    return java.sql.DatabaseMetaData.importedKeyNoAction;
  }

  public ResultSet getExportedKeys(String catalog, String schema, String table)
      throws SQLException {
    throw new SQLFeatureNotSupportedException(
        Constants.SQLExceptionMessages.SQL_FEATURE_NOT_SUPPORTED);
  }

  public ResultSet getCrossReference(String parentCatalog, String parentSchema, String parentTable,
      String foreignCatalog, String foreignSchema, String foreignTable) throws SQLException {
    throw new SQLFeatureNotSupportedException(
        Constants.SQLExceptionMessages.SQL_FEATURE_NOT_SUPPORTED);
  }

  public ResultSet getTypeInfo() throws SQLException {
    String[] columnNames = {"TYPE_NAME", "DATA_TYPE", "PRECISION", "LITERAL_PREFIX",
        "LITERAL_SUFFIX", "CREATE_PARAMS", "NULLABLE", "CASE_SENSITIVE", "SEARCHABLE",
        "UNSIGNED_ATTRIBUTE", "FIXED_PREC_SCALE", "AUTO_INCREMENT", "LOCAL_TYPE_NAME",
        "MINIMUM_SCALE", "MAXIMUM_SCALE", "SQL_DATA_TYPE", "SQL_DATETIME_SUB", "NUM_PREC_RADIX"};
    Query.Type[] columnTypes = {Query.Type.VARCHAR, Query.Type.INT32, Query.Type.INT32,
        Query.Type.VARCHAR, Query.Type.VARCHAR, Query.Type.VARCHAR, Query.Type.INT32,
        Query.Type.BIT, Query.Type.INT16, Query.Type.BIT, Query.Type.BIT, Query.Type.BIT,
        Query.Type.VARCHAR, Query.Type.INT16, Query.Type.INT16, Query.Type.INT32, Query.Type.INT32,
        Query.Type.INT32};

    String[][] data = {
        {"BIT", "-7", "1", "", "", "", "1", "true", "3", "false", "false", "false", "BIT", "0", "0",
            "0", "0", "10"},
        {"BOOL", "-7", "1", "", "", "", "1", "true", "3", "false", "false", "false", "BOOL", "0",
            "0", "0", "0", "10"},
        {"TINYINT", "-6", "3", "", "", "[(M)] [UNSIGNED] [ZEROFILL]", "1", "false", "3", "true",
            "false", "true", "TINYINT", "0", "0", "0", "0", "10"},
        {"TINYINT UNSIGNED", "-6", "3", "", "", "[(M)] [UNSIGNED] [ZEROFILL]", "1", "false", "3",
            "true", "false", "true", "TINYINT UNSIGNED", "0", "0", "0", "0", "10"},
        {"BIGINT", "-5", "19", "", "", "[(M)] [UNSIGNED] [ZEROFILL]", "1", "false", "3", "true",
            "false", "true", "BIGINT", "0", "0", "0", "0", "10"},
        {"BIGINT UNSIGNED", "-5", "20", "", "", "[(M)] [ZEROFILL]", "1", "false", "3", "true",
            "false", "true", "BIGINT UNSIGNED", "0", "0", "0", "0", "10"},
        {"LONG VARBINARY", "-4", "16777215", "'", "'", "", "1", "true", "3", "false", "false",
            "false", "LONG VARBINARY", "0", "0", "0", "0", "10"},
        {"MEDIUMBLOB", "-4", "16777215", "'", "'", "", "1", "true", "3", "false", "false", "false",
            "MEDIUMBLOB", "0", "0", "0", "0", "10"},
        {"LONGBLOB", "-4", "2147483647", "'", "'", "", "1", "true", "3", "false", "false", "false",
            "LONGBLOB", "0", "0", "0", "0", "10"},
        {"BLOB", "-4", "65535", "'", "'", "", "1", "true", "3", "false", "false", "false", "BLOB",
            "0", "0", "0", "0", "10"},
        {"TINYBLOB", "-4", "255", "'", "'", "", "1", "true", "3", "false", "false", "false",
            "TINYBLOB", "0", "0", "0", "0", "10"},
        {"VARBINARY", "-3", "65535", "'", "'", "(M)", "1", "true", "3", "false", "false", "false",
            "VARBINARY", "0", "0", "0", "0", "10"},
        {"BINARY", "-2", "255", "'", "'", "(M)", "1", "true", "3", "false", "false", "false",
            "BINARY", "0", "0", "0", "0", "10"},
        {"LONG VARCHAR", "-1", "16777215", "'", "'", "", "1", "false", "3", "false", "false",
            "false", "LONG VARCHAR", "0", "0", "0", "0", "10"},
        {"MEDIUMTEXT", "-1", "16777215", "'", "'", "", "1", "false", "3", "false", "false", "false",
            "MEDIUMTEXT", "0", "0", "0", "0", "10"},
        {"LONGTEXT", "-1", "2147483647", "'", "'", "", "1", "false", "3", "false", "false", "false",
            "LONGTEXT", "0", "0", "0", "0", "10"},
        {"TEXT", "-1", "65535", "'", "'", "", "1", "false", "3", "false", "false", "false", "TEXT",
            "0", "0", "0", "0", "10"},
        {"TINYTEXT", "-1", "255", "'", "'", "", "1", "false", "3", "false", "false", "false",
            "TINYTEXT", "0", "0", "0", "0", "10"},
        {"CHAR", "1", "255", "'", "'", "(M)", "1", "false", "3", "false", "false", "false", "CHAR",
            "0", "0", "0", "0", "10"},
        {"NUMERIC", "2", "65", "", "", "[(M[,D])] [ZEROFILL]", "1", "false", "3", "false", "false",
            "true", "NUMERIC", "-308", "308", "0", "0", "10"},
        {"DECIMAL", "3", "65", "", "", "[(M[,D])] [ZEROFILL]", "1", "false", "3", "false", "false",
            "true", "DECIMAL", "-308", "308", "0", "0", "10"},
        {"INTEGER", "4", "10", "", "", "[(M)] [UNSIGNED] [ZEROFILL]", "1", "false", "3", "true",
            "false", "true", "INTEGER", "0", "0", "0", "0", "10"},
        {"INTEGER UNSIGNED", "4", "10", "", "", "[(M)] [ZEROFILL]", "1", "false", "3", "true",
            "false", "true", "INTEGER UNSIGNED", "0", "0", "0", "0", "10"},
        {"INT", "4", "10", "", "", "[(M)] [UNSIGNED] [ZEROFILL]", "1", "false", "3", "true",
            "false", "true", "INT", "0", "0", "0", "0", "10"},
        {"INT UNSIGNED", "4", "10", "", "", "[(M)] [ZEROFILL]", "1", "false", "3", "true", "false",
            "true", "INT UNSIGNED", "0", "0", "0", "0", "10"},
        {"MEDIUMINT", "4", "7", "", "", "[(M)] [UNSIGNED] [ZEROFILL]", "1", "false", "3", "true",
            "false", "true", "MEDIUMINT", "0", "0", "0", "0", "10"},
        {"MEDIUMINT UNSIGNED", "4", "8", "", "", "[(M)] [ZEROFILL]", "1", "false", "3", "true",
            "false", "true", "MEDIUMINT UNSIGNED", "0", "0", "0", "0", "10"},
        {"SMALLINT", "5", "5", "", "", "[(M)] [UNSIGNED] [ZEROFILL]", "1", "false", "3", "true",
            "false", "true", "SMALLINT", "0", "0", "0", "0", "10"},
        {"SMALLINT UNSIGNED", "5", "5", "", "", "[(M)] [ZEROFILL]", "1", "false", "3", "true",
            "false", "true", "SMALLINT UNSIGNED", "0", "0", "0", "0", "10"},
        {"FLOAT", "7", "10", "", "", "[(M,D)] [ZEROFILL]", "1", "false", "3", "false", "false",
            "true", "FLOAT", "-38", "38", "0", "0", "10"},
        {"DOUBLE", "8", "17", "", "", "[(M,D)] [ZEROFILL]", "1", "false", "3", "false", "false",
            "true", "DOUBLE", "-308", "308", "0", "0", "10"},
        {"DOUBLE PRECISION", "8", "17", "", "", "[(M,D)] [ZEROFILL]", "1", "false", "3", "false",
            "false", "true", "DOUBLE PRECISION", "-308", "308", "0", "0", "10"},
        {"REAL", "8", "17", "", "", "[(M,D)] [ZEROFILL]", "1", "false", "3", "false", "false",
            "true", "REAL", "-308", "308", "0", "0", "10"},
        {"VARCHAR", "12", "65535", "'", "'", "(M)", "1", "false", "3", "false", "false", "false",
            "VARCHAR", "0", "0", "0", "0", "10"},
        {"ENUM", "12", "65535", "'", "'", "", "1", "false", "3", "false", "false", "false", "ENUM",
            "0", "0", "0", "0", "10"},
        {"SET", "12", "64", "'", "'", "", "1", "false", "3", "false", "false", "false", "SET", "0",
            "0", "0", "0", "10"},
        {"DATE", "91", "0", "'", "'", "", "1", "false", "3", "false", "false", "false", "DATE", "0",
            "0", "0", "0", "10"},
        {"TIME", "92", "0", "'", "'", "", "1", "false", "3", "false", "false", "false", "TIME", "0",
            "0", "0", "0", "10"},
        {"DATETIME", "93", "0", "'", "'", "", "1", "false", "3", "false", "false", "false",
            "DATETIME", "0", "0", "0", "0", "10"},
        {"TIMESTAMP", "93", "0", "'", "'", "[(M)]", "1", "false", "3", "false", "false", "false",
            "TIMESTAMP", "0", "0", "0", "0", "10"}};

    return new VitessResultSet(columnNames, columnTypes, data, this.connection);
  }

  @SuppressWarnings("StringBufferReplaceableByString")
  public ResultSet getIndexInfo(String catalog, String schema, String table, boolean unique,
      boolean approximate) throws SQLException {

    ArrayList<ArrayList<String>> data = new ArrayList<>();
    final SortedMap<IndexMetaDataKey, ArrayList<String>> sortedRows = new TreeMap<>();
    VitessStatement vitessStatement = new VitessStatement(this.connection);
    ResultSet resultSet = null;
    try {
      resultSet = vitessStatement.executeQuery(
          "SHOW INDEX FROM " + this.quotedId + table + this.quotedId + " " + "FROM " + this.quotedId
              + catalog + this.quotedId);

      while (resultSet.next()) {
        ArrayList<String> row = new ArrayList<>();
        row.add(0, catalog);
        row.add(1, null);
        row.add(2, resultSet.getString("Table"));

        boolean indexIsUnique = resultSet.getInt("Non_unique") == 0;

        row.add(3, !indexIsUnique ? "true" : "false");
        row.add(4, "");
        row.add(5, resultSet.getString("Key_name"));
        short indexType = DatabaseMetaData.tableIndexOther;
        row.add(6, Integer.toString(indexType));
        row.add(7, resultSet.getString("Seq_in_index"));
        row.add(8, resultSet.getString("Column_name"));
        row.add(9, resultSet.getString("Collation"));

        long cardinality = resultSet.getLong("Cardinality");
        if (cardinality > Integer.MAX_VALUE) {
          cardinality = Integer.MAX_VALUE;
        }

        row.add(10, String.valueOf(cardinality));
        row.add(11, "0");
        row.add(12, null);

        IndexMetaDataKey indexInfoKey = new IndexMetaDataKey(!indexIsUnique, indexType,
            resultSet.getString("Key_name").toLowerCase(), resultSet.getShort("Seq_in_index"));
        sortedRows.put(indexInfoKey, row);
      }

      for (ArrayList<String> row : sortedRows.values()) {
        data.add(row);
      }

    } finally {
      if (null != resultSet) {
        resultSet.close();
      }
      vitessStatement.close();
    }
    String[] columnName = new String[]{"TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "Non_unique",
        "INDEX_QUALIFIER", "INDEX_NAME", "TYPE", "ORDINAL_POSITION", "COLUMN_NAME", "ASC_OR_DESC",
        "CARDINALITY", "PAGES", "FILTER_CONDITION"};

    Query.Type[] columnType = new Query.Type[]{Query.Type.CHAR, Query.Type.CHAR, Query.Type.CHAR,
        Query.Type.BIT, Query.Type.CHAR, Query.Type.CHAR, Query.Type.INT16, Query.Type.INT16,
        Query.Type.CHAR, Query.Type.CHAR, Query.Type.INT32, Query.Type.INT32, Query.Type.CHAR};

    return new VitessResultSet(columnName, columnType, data, this.connection);
  }

  public boolean ownUpdatesAreVisible(int type) throws SQLException {
    return false;
  }

  public boolean ownDeletesAreVisible(int type) throws SQLException {
    return false;
  }

  public boolean ownInsertsAreVisible(int type) throws SQLException {
    return false;
  }

  public boolean othersUpdatesAreVisible(int type) throws SQLException {
    return false;
  }

  public boolean othersDeletesAreVisible(int type) throws SQLException {
    return false;
  }

  public boolean othersInsertsAreVisible(int type) throws SQLException {
    return false;
  }

  public boolean updatesAreDetected(int type) throws SQLException {
    return false;
  }

  public boolean deletesAreDetected(int type) throws SQLException {
    return false;
  }

  public ResultSet getUDTs(String catalog, String schemaPattern, String typeNamePattern,
      int[] types) throws SQLException {
    String[] columnNames = {"TYPE_CAT", "TYPE_SCHEM", "TYPE_NAME", "CLASS_NAME", "DATA_TYPE",
        "REMARKS", "BASE_TYPE"};
    Query.Type[] columnType = {Query.Type.VARCHAR, Query.Type.INT32, Query.Type.VARCHAR,
        Query.Type.VARCHAR, Query.Type.INT32, Query.Type.VARCHAR, Query.Type.INT16};
    return new VitessResultSet(columnNames, columnType, new String[][]{}, this.connection);
  }

  public ResultSet getSuperTypes(String catalog, String schemaPattern, String typeNamePattern)
      throws SQLException {
    String[] columnNames = {"TYPE_CAT", "TYPE_SCHEM", "TYPE_NAME", "SUPERTYPE_CAT",
        "SUPERTYPE_SCHEM", "SUPERTYPE_NAME"};
    Query.Type[] columnType = {Query.Type.CHAR, Query.Type.CHAR, Query.Type.CHAR, Query.Type.CHAR,
        Query.Type.CHAR, Query.Type.CHAR};
    return new VitessResultSet(columnNames, columnType, new String[][]{}, this.connection);
  }

  public ResultSet getSuperTables(String catalog, String schemaPattern, String tableNamePattern)
      throws SQLException {
    String[] columnNames = {"TABLE_CAT", "TYPE_SCHEM", "TABLE_NAME", "SUPERTABLE_NAME"};
    Query.Type[] columnType = {Query.Type.CHAR, Query.Type.CHAR, Query.Type.CHAR, Query.Type.CHAR};
    return new VitessResultSet(columnNames, columnType, new String[][]{}, this.connection);
  }

  public ResultSet getAttributes(String catalog, String schemaPattern, String typeNamePattern,
      String attributeNamePattern) throws SQLException {

    String[] columnNames = {"TYPE_CAT", "TYPE_SCHEM", "TYPE_NAME", "ATTR_NAME", "DATA_TYPE",
        "ATTR_TYPE_NAME", "ATTR_SIZE", "DECIMAL_DIGITS", "NUM_PREC_RADIX", "NULLABLE", "REMARKS",
        "ATTR_DEF", "SQL_DATA_TYPE", "SQL_DATETIME_SUB", "CHAR_OCTET_LENGTH", "ORDINAL_POSITION",
        "ISNULLABLE", "SCOPE_CATALOG", "SCOPE_SCHEMA", "SCOPE_TABLE", "SOURCE_DATA_TYPE"};
    Query.Type[] columnType = {Query.Type.CHAR, Query.Type.CHAR, Query.Type.CHAR, Query.Type.CHAR,
        Query.Type.INT16, Query.Type.CHAR, Query.Type.INT32, Query.Type.INT32, Query.Type.INT32,
        Query.Type.INT32, Query.Type.CHAR, Query.Type.CHAR, Query.Type.INT32, Query.Type.INT32,
        Query.Type.INT32, Query.Type.INT32, Query.Type.CHAR, Query.Type.CHAR, Query.Type.CHAR,
        Query.Type.CHAR, Query.Type.INT16};
    return new VitessResultSet(columnNames, columnType, new String[][]{}, this.connection);
  }

  public int getSQLStateType() throws SQLException {
    return DatabaseMetaData.sqlStateSQL99;
  }

  public boolean locatorsUpdateCopy() throws SQLException {
    return true;
  }

  public RowIdLifetime getRowIdLifetime() throws SQLException {
    throw new SQLFeatureNotSupportedException(
        Constants.SQLExceptionMessages.SQL_FEATURE_NOT_SUPPORTED);
  }

  public ResultSet getSchemas(String catalog, String schemaPattern) throws SQLException {
    String[] columnNames = {"TABLE_CAT", "TABLE_CATALOG"};
    Query.Type[] columnType = {Query.Type.CHAR, Query.Type.CHAR};
    return new VitessResultSet(columnNames, columnType, new String[][]{}, this.connection);
  }

  public ResultSet getClientInfoProperties() throws SQLException {
    String[] columnNames = {"NAME", "MAX_LEN", "DEFAULT_VALUE", "DESCRIPTION"};
    Query.Type[] columnType = {Query.Type.VARCHAR, Query.Type.INT32, Query.Type.VARCHAR,
        Query.Type.VARCHAR};
    return new VitessResultSet(columnNames, columnType, new String[][]{}, this.connection);
  }

  public ResultSet getFunctions(String catalog, String schemaPattern, String functionNamePattern)
      throws SQLException {
    throw new SQLFeatureNotSupportedException(
        Constants.SQLExceptionMessages.SQL_FEATURE_NOT_SUPPORTED);
  }

  public ResultSet getFunctionColumns(String catalog, String schemaPattern,
      String functionNamePattern, String columnNamePattern) throws SQLException {
    throw new SQLFeatureNotSupportedException(
        Constants.SQLExceptionMessages.SQL_FEATURE_NOT_SUPPORTED);
  }

  public ResultSet getPseudoColumns(String catalog, String schemaPattern, String tableNamePattern,
      String columnNamePattern) throws SQLException {
    String[] columnNames = {"TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "COLUMN_NAME", "DATA_TYPE",
        "COLUMN_SIZE", "DECIMAL_DIGITS", "NUM_PREC_RADIX", "COLUMN_USAGE", "REMARKS",
        "CHAR_OCTET_LENGTH", "IS_NULLABLE"};
    Query.Type[] columnType = {Query.Type.VARCHAR, Query.Type.VARCHAR, Query.Type.VARCHAR,
        Query.Type.VARCHAR, Query.Type.INT32, Query.Type.INT32, Query.Type.INT32, Query.Type.INT32,
        Query.Type.VARCHAR, Query.Type.VARCHAR, Query.Type.INT32, Query.Type.VARCHAR};
    return new VitessResultSet(columnNames, columnType, new String[][]{}, this.connection);
  }

  public <T> T unwrap(Class<T> iface) throws SQLException {
    return null;
  }

  public boolean isWrapperFor(Class<?> iface) throws SQLException {
    return false;
  }

  /**
   * Enumeration for Table Types
   */
  protected enum TableType {
    LOCAL_TEMPORARY("LOCAL TEMPORARY"),
    SYSTEM_TABLE("SYSTEM TABLE"),
    SYSTEM_VIEW("SYSTEM VIEW"),
    TABLE("TABLE", new String[]{"BASE TABLE"}),
    VIEW("VIEW"),
    UNKNOWN("UNKNOWN"),
    ;

    private String name;
    private byte[] nameAsBytes;
    private String[] synonyms;

    TableType(String tableTypeName) {
      this(tableTypeName, null);
    }

    TableType(String tableTypeName, String[] tableTypeSynonyms) {
      this.name = tableTypeName;
      this.nameAsBytes = tableTypeName.getBytes();
      this.synonyms = tableTypeSynonyms;
    }

    static TableType getTableTypeEqualTo(String tableTypeName) {
      for (TableType tableType : TableType.values()) {
        if (tableType.equalsTo(tableTypeName)) {
          return tableType;
        }
      }
      return UNKNOWN;
    }

    static TableType getTableTypeCompliantWith(String tableTypeName) {
      for (TableType tableType : TableType.values()) {
        if (tableType.compliesWith(tableTypeName)) {
          return tableType;
        }
      }
      return UNKNOWN;
    }

    String getName() {
      return this.name;
    }

    boolean equalsTo(String tableTypeName) {
      return this.name.equalsIgnoreCase(tableTypeName);
    }

    boolean compliesWith(String tableTypeName) {
      if (equalsTo(tableTypeName)) {
        return true;
      }
      if (null != this.synonyms) {
        for (String synonym : this.synonyms) {
          if (synonym.equalsIgnoreCase(tableTypeName)) {
            return true;
          }
        }
      }
      return false;
    }
  }


  /**
   * Helper class to provide means of comparing tables by TABLE_TYPE, TABLE_CAT, TABLE_SCHEM and
   * TABLE_NAME.
   */
  protected class TableMetaDataKey implements Comparable<TableMetaDataKey> {

    String tableType;
    String tableCat;
    String tableSchem;
    String tableName;

    TableMetaDataKey(String tableType, String tableCat, String tableSchem, String tableName) {
      this.tableType = tableType == null ? "" : tableType;
      this.tableCat = tableCat == null ? "" : tableCat;
      this.tableSchem = tableSchem == null ? "" : tableSchem;
      this.tableName = tableName == null ? "" : tableName;
    }

    public int compareTo(TableMetaDataKey tablesKey) {
      int compareResult;

      if ((compareResult = this.tableType.compareTo(tablesKey.tableType)) != 0) {
        return compareResult;
      }
      if ((compareResult = this.tableCat.compareTo(tablesKey.tableCat)) != 0) {
        return compareResult;
      }
      if ((compareResult = this.tableSchem.compareTo(tablesKey.tableSchem)) != 0) {
        return compareResult;
      }
      return this.tableName.compareTo(tablesKey.tableName);
    }

    @Override
    public boolean equals(Object obj) {
      if (null == obj) {
        return false;
      }

      if (obj == this) {
        return true;
      }

      return obj instanceof TableMetaDataKey && compareTo((TableMetaDataKey) obj) == 0;
    }
  }


  /**
   * Helper class to provide means of comparing indexes by Non_unique, TYPE, INDEX_NAME, and
   * ORDINAL_POSITION.
   */
  protected class IndexMetaDataKey implements Comparable<IndexMetaDataKey> {

    Boolean columnNonUnique;
    Short columnType;
    String columnIndexName;
    Short columnOrdinalPosition;

    IndexMetaDataKey(boolean columnNonUnique, short columnType, String columnIndexName,
        short columnOrdinalPosition) {
      this.columnNonUnique = columnNonUnique;
      this.columnType = columnType;
      this.columnIndexName = columnIndexName;
      this.columnOrdinalPosition = columnOrdinalPosition;
    }

    public int compareTo(IndexMetaDataKey indexInfoKey) {
      int compareResult;

      if ((compareResult = this.columnNonUnique.compareTo(indexInfoKey.columnNonUnique)) != 0) {
        return compareResult;
      }
      if ((compareResult = this.columnType.compareTo(indexInfoKey.columnType)) != 0) {
        return compareResult;
      }
      if ((compareResult = this.columnIndexName.compareTo(indexInfoKey.columnIndexName)) != 0) {
        return compareResult;
      }
      return this.columnOrdinalPosition.compareTo(indexInfoKey.columnOrdinalPosition);
    }

    @Override
    public boolean equals(Object obj) {
      if (null == obj) {
        return false;
      }

      if (obj == this) {
        return true;
      }

      return obj instanceof IndexMetaDataKey && compareTo((IndexMetaDataKey) obj) == 0;
    }
  }


  /**
   * Parses and represents common data type information used by various column/parameter methods.
   */
  class TypeDescriptor {

    int bufferLength;

    int charOctetLength;

    Integer columnSize;

    short dataType;

    Integer decimalDigits;

    String isNullable;

    int nullability;

    int numPrecRadix = 10;

    String typeName;

    TypeDescriptor(String typeInfo, String nullabilityInfo) throws SQLException {
      if (typeInfo == null) {
        throw new SQLException("NULL typeinfo not supported.");
      }

      String mysqlType;
      String fullMysqlType;

      if (typeInfo.indexOf("(") != -1) {
        mysqlType = typeInfo.substring(0, typeInfo.indexOf("(")).trim();
      } else {
        mysqlType = typeInfo;
      }

      int indexOfUnsignedInMysqlType = StringUtils.indexOfIgnoreCase(mysqlType, "unsigned");

      if (indexOfUnsignedInMysqlType != -1) {
        mysqlType = mysqlType.substring(0, (indexOfUnsignedInMysqlType - 1));
      }

      // Add unsigned to typename reported to enduser as 'native type', if present

      boolean isUnsigned = false;

      if ((StringUtils.indexOfIgnoreCase(typeInfo, "unsigned") != -1) && (
          StringUtils.indexOfIgnoreCase(typeInfo, "set") != 0) && (
          StringUtils.indexOfIgnoreCase(typeInfo, "enum") != 0)) {
        fullMysqlType = mysqlType + " unsigned";
        isUnsigned = true;
      } else {
        fullMysqlType = mysqlType;
      }
      fullMysqlType = fullMysqlType.toUpperCase(Locale.ENGLISH);

      this.dataType = (short) MysqlDefs.mysqlToJavaType(mysqlType);

      this.typeName = fullMysqlType;

      // Figure Out the Size

      if (StringUtils.startsWithIgnoreCase(typeInfo, "enum")) {
        String temp = typeInfo.substring(typeInfo.indexOf("("), typeInfo.lastIndexOf(")"));
        StringTokenizer tokenizer = new StringTokenizer(temp, ",");
        int maxLength = 0;

        while (tokenizer.hasMoreTokens()) {
          maxLength = Math.max(maxLength, (tokenizer.nextToken().length() - 2));
        }

        this.columnSize = maxLength;
        this.decimalDigits = null;
      } else if (StringUtils.startsWithIgnoreCase(typeInfo, "set")) {
        String temp = typeInfo.substring(typeInfo.indexOf("(") + 1, typeInfo.lastIndexOf(")"));
        StringTokenizer tokenizer = new StringTokenizer(temp, ",");
        int maxLength = 0;

        int numElements = tokenizer.countTokens();

        if (numElements > 0) {
          maxLength += (numElements - 1);
        }

        while (tokenizer.hasMoreTokens()) {
          String setMember = tokenizer.nextToken().trim();

          if (setMember.startsWith(Constants.LITERAL_SINGLE_QUOTE) && setMember
              .endsWith(Constants.LITERAL_SINGLE_QUOTE)) {
            maxLength += setMember.length() - 2;
          } else {
            maxLength += setMember.length();
          }
        }

        this.columnSize = maxLength;
        this.decimalDigits = null;
      } else if (typeInfo.indexOf(",") != -1) {
        // Numeric with decimals
        this.columnSize = Integer.valueOf(
            typeInfo.substring((typeInfo.indexOf("(") + 1), (typeInfo.indexOf(","))).trim());
        this.decimalDigits = Integer.valueOf(
            typeInfo.substring((typeInfo.indexOf(",") + 1), (typeInfo.indexOf(")"))).trim());
      } else {
        this.columnSize = null;
        this.decimalDigits = null;

        /* If the size is specified with the DDL, use that */
        if ((StringUtils.indexOfIgnoreCase(typeInfo, "char") != -1
            || StringUtils.indexOfIgnoreCase(typeInfo, "text") != -1
            || StringUtils.indexOfIgnoreCase(typeInfo, "blob") != -1
            || StringUtils.indexOfIgnoreCase(typeInfo, "binary") != -1
            || StringUtils.indexOfIgnoreCase(typeInfo, "bit") != -1)
            && typeInfo.indexOf("(") != -1) {
          int endParenIndex = typeInfo.indexOf(")");

          if (endParenIndex == -1) {
            endParenIndex = typeInfo.length();
          }

          this.columnSize = Integer
              .valueOf(typeInfo.substring((typeInfo.indexOf("(") + 1), endParenIndex).trim());

          // Adjust for pseudo-boolean
          if (this.columnSize == 1 && StringUtils.startsWithIgnoreCase(typeInfo, "tinyint")) {
            this.dataType = Types.BIT;
            this.typeName = "BIT";
          }
        } else if (StringUtils.startsWithIgnoreCase(typeInfo, "tinyint")) {
          if (typeInfo.indexOf("(1)") != -1) {
            this.dataType = Types.BIT;
            this.typeName = "BIT";

          } else {
            this.columnSize = 3;
            this.decimalDigits = 0;
          }
        } else if (StringUtils.startsWithIgnoreCase(typeInfo, "smallint")) {
          this.columnSize = 5;
          this.decimalDigits = 0;
        } else if (StringUtils.startsWithIgnoreCase(typeInfo, "mediumint")) {
          this.columnSize = isUnsigned ? 8 : 7;
          this.decimalDigits = 0;
        } else if (StringUtils.startsWithIgnoreCase(typeInfo, "int")) {
          this.columnSize = 10;
          this.decimalDigits = 0;
        } else if (StringUtils.startsWithIgnoreCase(typeInfo, "integer")) {
          this.columnSize = 10;
          this.decimalDigits = 0;
        } else if (StringUtils.startsWithIgnoreCase(typeInfo, "bigint")) {
          this.dataType = Types.BIGINT;
          this.columnSize = isUnsigned ? 20 : 19;
          this.decimalDigits = 0;
        } else if (StringUtils.startsWithIgnoreCase(typeInfo, "int24")) {
          this.columnSize = 19;
          this.decimalDigits = 0;
        } else if (StringUtils.startsWithIgnoreCase(typeInfo, "real")) {
          this.columnSize = 12;
        } else if (StringUtils.startsWithIgnoreCase(typeInfo, "float")) {
          this.columnSize = 12;
        } else if (StringUtils.startsWithIgnoreCase(typeInfo, "decimal")) {
          this.columnSize = 12;
        } else if (StringUtils.startsWithIgnoreCase(typeInfo, "numeric")) {
          this.columnSize = 12;
        } else if (StringUtils.startsWithIgnoreCase(typeInfo, "double")) {
          this.columnSize = 22;
        } else if (StringUtils.startsWithIgnoreCase(typeInfo, "char")) {
          this.columnSize = 1;
        } else if (StringUtils.startsWithIgnoreCase(typeInfo, "varchar")) {
          this.columnSize = 255;
        } else if (StringUtils.startsWithIgnoreCase(typeInfo, "timestamp")) {
          this.columnSize = 19;
        } else if (StringUtils.startsWithIgnoreCase(typeInfo, "datetime")) {
          this.columnSize = 19;
        } else if (StringUtils.startsWithIgnoreCase(typeInfo, "date")) {
          this.columnSize = 10;
        } else if (StringUtils.startsWithIgnoreCase(typeInfo, "time")) {
          this.columnSize = 8;

        } else if (StringUtils.startsWithIgnoreCase(typeInfo, "tinyblob")) {
          this.columnSize = 255;
        } else if (StringUtils.startsWithIgnoreCase(typeInfo, "blob")) {
          this.columnSize = 65535;
        } else if (StringUtils.startsWithIgnoreCase(typeInfo, "mediumblob")) {
          this.columnSize = 16777215;
        } else if (StringUtils.startsWithIgnoreCase(typeInfo, "longblob")) {
          this.columnSize = Integer.MAX_VALUE;
        } else if (StringUtils.startsWithIgnoreCase(typeInfo, "tinytext")) {
          this.columnSize = 255;
        } else if (StringUtils.startsWithIgnoreCase(typeInfo, "text")) {
          this.columnSize = 65535;
        } else if (StringUtils.startsWithIgnoreCase(typeInfo, "mediumtext")) {
          this.columnSize = 16777215;
        } else if (StringUtils.startsWithIgnoreCase(typeInfo, "longtext")) {
          this.columnSize = Integer.MAX_VALUE;
        } else if (StringUtils.startsWithIgnoreCase(typeInfo, "enum")) {
          this.columnSize = 255;
        } else if (StringUtils.startsWithIgnoreCase(typeInfo, "set")) {
          this.columnSize = 255;
        }

      }
      this.bufferLength = 65535;
      this.numPrecRadix = 10;

      if (nullabilityInfo != null) {
        switch (nullabilityInfo) {
          case "YES":
            this.nullability = DatabaseMetaData.columnNullable;
            this.isNullable = "YES";

            break;
          case "UNKNOWN":
            this.nullability = DatabaseMetaData.columnNullableUnknown;
            this.isNullable = "";
            break;
          default:
            this.nullability = DatabaseMetaData.columnNoNulls;
            this.isNullable = "NO";
            break;
        }
      } else {
        this.nullability = DatabaseMetaData.columnNoNulls;
        this.isNullable = "NO";
      }
    }
  }
}
