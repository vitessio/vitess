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

import io.vitess.util.Constants;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.logging.Logger;

/**
 * Created by harshit.gangal on 25/01/16.
 */
public abstract class VitessDatabaseMetaData implements DatabaseMetaData {

  private static final String SEARCH_STRING_ESCAPE = "\\";
  private static final String EXTRA_NAME_CHARS = "#@";
  private static final String SCHEMA_TERM = "";
  private static final String CATALOG_SEPARATOR = ".";
  private static final String PROCEDURE_TERM = "procedure";
  private static final String CATALOG_TERM = "database";
  private static final String DATABASE_PRODUCT_NAME = "MySQL";
  /* Get actual class name to be printed on */
  private static Logger logger = Logger.getLogger(VitessDatabaseMetaData.class.getName());
  protected final String quotedId = "`";
  protected VitessConnection connection = null;

  public String getURL() throws SQLException {
    if (this.connection == null || this.connection.getUrl() == null) {
      return null;
    }
    return this.connection.getUrl().getUrl();
  }

  public String getUserName() throws SQLException {
    return this.connection.getUsername();
  }

  public boolean isReadOnly() throws SQLException {
    return this.connection.isReadOnly();
  }

  public boolean allProceduresAreCallable() throws SQLException {
    return false;
  }

  public boolean allTablesAreSelectable() throws SQLException {
    return false;
  }

  public boolean nullsAreSortedHigh() throws SQLException {
    return false;
  }

  public boolean nullsAreSortedLow() throws SQLException {
    return !this.nullsAreSortedHigh();
  }

  public String getDatabaseProductName() throws SQLException {
    return DATABASE_PRODUCT_NAME;
  }

  public String getDatabaseProductVersion() throws SQLException {
    return this.connection.getDbProperties().getProductVersion();
  }

  public String getDriverVersion() throws SQLException {
    return Constants.DRIVER_MAJOR_VERSION + "." + Constants.DRIVER_MINOR_VERSION;
  }

  public int getDriverMajorVersion() {
    return Constants.DRIVER_MAJOR_VERSION;
  }

  public int getDriverMinorVersion() {
    return Constants.DRIVER_MINOR_VERSION;
  }

  public boolean usesLocalFiles() throws SQLException {
    return false;
  }

  public boolean usesLocalFilePerTable() throws SQLException {
    return false;
  }

  public boolean supportsMixedCaseIdentifiers() throws SQLException {
    return !connection.getDbProperties().getUseCaseInsensitiveComparisons();
  }

  public boolean storesUpperCaseIdentifiers() throws SQLException {
    return false;
  }

  public boolean storesLowerCaseIdentifiers() throws SQLException {
    return connection.getDbProperties().getStoresLowerCaseTableName();
  }

  public boolean storesMixedCaseIdentifiers() throws SQLException {
    return !connection.getDbProperties().getStoresLowerCaseTableName();
  }

  public boolean supportsMixedCaseQuotedIdentifiers() throws SQLException {
    return !connection.getDbProperties().getUseCaseInsensitiveComparisons();
  }

  public boolean storesUpperCaseQuotedIdentifiers() throws SQLException {
    return false;
  }

  public boolean storesLowerCaseQuotedIdentifiers() throws SQLException {
    return connection.getDbProperties().getStoresLowerCaseTableName();
  }

  public boolean storesMixedCaseQuotedIdentifiers() throws SQLException {
    return !connection.getDbProperties().getStoresLowerCaseTableName();
  }

  public String getNumericFunctions() throws SQLException {
    return
        "ABS,ACOS,ASIN,ATAN,ATAN2,BIT_COUNT,CEILING,COS,COT,DEGREES,EXP,FLOOR,LOG,LOG10,MAX,MIN,"
            + "MOD,PI,POW,"
            + "POWER,RADIANS,RAND,ROUND,SIN,SQRT,TAN,TRUNCATE";
  }

  public String getStringFunctions() throws SQLException {
    return
        "ASCII,BIN,BIT_LENGTH,CHAR,CHARACTER_LENGTH,CHAR_LENGTH,CONCAT,CONCAT_WS,CONV,ELT,"
            + "EXPORT_SET,FIELD,"
            + "FIND_IN_SET,HEX,INSERT,INSTR,LCASE,LEFT,LENGTH,LOAD_FILE,LOCATE,LOCATE,LOWER,LPAD,"
            + "LTRIM,MAKE_SET,MATCH,"
            + "MID,OCT,OCTET_LENGTH,ORD,POSITION,QUOTE,REPEAT,REPLACE,REVERSE,RIGHT,RPAD,RTRIM,"
            + "SOUNDEX,SPACE,STRCMP,"
            + "SUBSTRING,SUBSTRING,SUBSTRING,SUBSTRING,SUBSTRING_INDEX,TRIM,UCASE,UPPER";
  }

  public String getSystemFunctions() throws SQLException {
    return "DATABASE,USER,SYSTEM_USER,SESSION_USER,LAST_INSERT_ID,VERSION";
  }

  public String getTimeDateFunctions() throws SQLException {
    return
        "DAYOFWEEK,WEEKDAY,DAYOFMONTH,DAYOFYEAR,MONTH,DAYNAME,MONTHNAME,QUARTER,WEEK,YEAR,HOUR,"
            + "MINUTE,SECOND,"
            + "PERIOD_ADD,PERIOD_DIFF,TO_DAYS,FROM_DAYS,DATE_FORMAT,TIME_FORMAT,CURDATE,"
            + "CURRENT_DATE,CURTIME,"
            + "CURRENT_TIME,NOW,SYSDATE,CURRENT_TIMESTAMP,UNIX_TIMESTAMP,FROM_UNIXTIME,"
            + "SEC_TO_TIME,TIME_TO_SEC";
  }

  public String getSearchStringEscape() throws SQLException {
    return SEARCH_STRING_ESCAPE;
  }

  public String getExtraNameCharacters() throws SQLException {
    return EXTRA_NAME_CHARS;
  }

  public boolean supportsAlterTableWithAddColumn() throws SQLException {
    return false;
  }

  public boolean supportsAlterTableWithDropColumn() throws SQLException {
    return false;
  }

  public boolean supportsColumnAliasing() throws SQLException {
    return true;
  }

  public boolean nullPlusNonNullIsNull() throws SQLException {
    return true;
  }

  public boolean supportsConvert() throws SQLException {
    return false;
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

  public boolean supportsExpressionsInOrderBy() throws SQLException {
    return false;
  }

  public boolean supportsOrderByUnrelated() throws SQLException {
    return false;
  }

  public boolean supportsGroupBy() throws SQLException {
    return false;
  }

  public boolean supportsGroupByUnrelated() throws SQLException {
    return false;
  }

  public boolean supportsGroupByBeyondSelect() throws SQLException {
    return false;
  }

  public boolean supportsLikeEscapeClause() throws SQLException {
    return true;
  }

  public boolean supportsMultipleResultSets() throws SQLException {
    return false;
  }

  public boolean supportsMultipleTransactions() throws SQLException {
    return true;
  }

  public boolean supportsNonNullableColumns() throws SQLException {
    return true;
  }

  public boolean supportsMinimumSQLGrammar() throws SQLException {
    return true;
  }

  public boolean supportsCoreSQLGrammar() throws SQLException {
    return false;
  }

  public boolean supportsExtendedSQLGrammar() throws SQLException {
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

  public boolean supportsOuterJoins() throws SQLException {
    return false;
  }

  public boolean supportsFullOuterJoins() throws SQLException {
    return false;
  }

  public boolean supportsLimitedOuterJoins() throws SQLException {
    return false;
  }

  public String getSchemaTerm() throws SQLException {
    return SCHEMA_TERM;
  }

  public String getProcedureTerm() throws SQLException {
    return PROCEDURE_TERM;
  }

  public String getCatalogTerm() throws SQLException {
    return CATALOG_TERM;
  }

  public boolean isCatalogAtStart() throws SQLException {
    return true;
  }

  public String getCatalogSeparator() throws SQLException {
    return CATALOG_SEPARATOR;
  }

  public VitessConnection getConnection() throws SQLException {
    return this.connection;
  }

  public void setConnection(VitessConnection vitessConnection) throws SQLException {
    this.connection = vitessConnection;
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

  public boolean supportsSchemasInIndexDefinitions() throws SQLException {
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

  public boolean supportsSelectForUpdate() throws SQLException {
    return false;
  }

  public boolean supportsStoredProcedures() throws SQLException {
    return false;
  }

  public boolean supportsSubqueriesInComparisons() throws SQLException {
    return false;
  }

  public boolean supportsSubqueriesInExists() throws SQLException {
    return false;
  }

  public boolean supportsSubqueriesInIns() throws SQLException {
    return false;
  }

  public boolean supportsSubqueriesInQuantifieds() throws SQLException {
    return false;
  }

  public boolean supportsCorrelatedSubqueries() throws SQLException {
    return false;
  }

  public boolean supportsUnion() throws SQLException {
    return false;
  }

  public boolean supportsUnionAll() throws SQLException {
    return false;
  }

  public boolean supportsOpenCursorsAcrossCommit() throws SQLException {
    return false;
  }

  public boolean supportsOpenCursorsAcrossRollback() throws SQLException {
    return false;
  }

  public boolean supportsOpenStatementsAcrossCommit() throws SQLException {
    return false;
  }

  public boolean supportsOpenStatementsAcrossRollback() throws SQLException {
    return false;
  }

  public int getMaxBinaryLiteralLength() throws SQLException {
    return 16777208;
  }

  public int getMaxCharLiteralLength() throws SQLException {
    return 16777208;
  }

  public int getMaxColumnNameLength() throws SQLException {
    return 64;
  }

  public int getMaxColumnsInGroupBy() throws SQLException {
    return 64;
  }

  public int getMaxColumnsInIndex() throws SQLException {
    return 16;
  }

  public int getMaxColumnsInOrderBy() throws SQLException {
    return 64;
  }

  public int getMaxColumnsInSelect() throws SQLException {
    return 256;
  }

  public int getMaxConnections() throws SQLException {
    return 0;
  }

  public int getMaxCursorNameLength() throws SQLException {
    return 0;
  }

  public int getMaxIndexLength() throws SQLException {
    return 256;
  }

  public boolean doesMaxRowSizeIncludeBlobs() throws SQLException {
    return false;
  }

  public int getMaxStatements() throws SQLException {
    return 0;
  }

  public int getMaxTableNameLength() throws SQLException {
    return 64;
  }

  public int getMaxTablesInSelect() throws SQLException {
    return 256;
  }

  public int getMaxUserNameLength() throws SQLException {
    return 16;
  }

  public int getMaxProcedureNameLength() throws SQLException {
    return 256;
  }

  public boolean supportsDataDefinitionAndDataManipulationTransactions() throws SQLException {
    return false;
  }

  public boolean supportsDataManipulationTransactionsOnly() throws SQLException {
    return false;
  }

  public boolean dataDefinitionCausesTransactionCommit() throws SQLException {
    return false;
  }

  public boolean dataDefinitionIgnoredInTransactions() throws SQLException {
    return false;
  }

  public String getIdentifierQuoteString() throws SQLException {
    return this.quotedId;
  }

  public ResultSet getProcedures(String catalog, String schemaPattern, String procedureNamePattern)
      throws SQLException {
    return null;
  }

  public ResultSet getProcedureColumns(String catalog, String schemaPattern,
      String procedureNamePattern, String columnNamePattern) throws SQLException {
    throw new SQLFeatureNotSupportedException(
        Constants.SQLExceptionMessages.SQL_FEATURE_NOT_SUPPORTED);
  }

  public boolean supportsResultSetType(int type) throws SQLException {
    return type == ResultSet.TYPE_FORWARD_ONLY;
  }

  public boolean supportsResultSetConcurrency(int type, int concurrency) throws SQLException {
    return (type == ResultSet.TYPE_FORWARD_ONLY && concurrency == ResultSet.CONCUR_READ_ONLY);
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

  public boolean insertsAreDetected(int type) throws SQLException {
    return false;
  }

  public boolean supportsBatchUpdates() throws SQLException {
    return true;
  }

  public boolean supportsSavepoints() throws SQLException {
    return false;
  }

  public boolean supportsNamedParameters() throws SQLException {
    return false;
  }

  public boolean supportsMultipleOpenResults() throws SQLException {
    return false;
  }

  public boolean supportsGetGeneratedKeys() throws SQLException {
    return true;
  }

  public boolean supportsResultSetHoldability(int holdability) throws SQLException {
    return false;
  }

  public int getResultSetHoldability() throws SQLException {
    return 0;
  }

  public int getDatabaseMajorVersion() throws SQLException {
    return Integer.valueOf(this.connection.getDbProperties().getMajorVersion());
  }

  public int getDatabaseMinorVersion() throws SQLException {
    return Integer.valueOf(this.connection.getDbProperties().getMinorVersion());
  }

  public int getJDBCMajorVersion() throws SQLException {
    return 1;
  }

  public int getJDBCMinorVersion() throws SQLException {
    return 0;
  }

  public boolean supportsStatementPooling() throws SQLException {
    return false;
  }

  public boolean supportsStoredFunctionsUsingCallSyntax() throws SQLException {
    return false;
  }

  public boolean autoCommitFailureClosesAllResultSets() throws SQLException {
    return false;
  }

  public boolean generatedKeyAlwaysReturned() throws SQLException {
    return true;
  }

  public <T> T unwrap(Class<T> iface) throws SQLException {
    return null;
  }

  public boolean isWrapperFor(Class<?> iface) throws SQLException {
    return false;
  }
}
