package io.vitess.jdbc;

import io.vitess.proto.Query;
import io.vitess.util.Constants;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.RowIdLifetime;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.logging.Logger;

/**
 * Created by ashudeep.sharma on 15/02/16.
 */
public class VitessMariaDBDatabaseMetadata extends VitessDatabaseMetaData
    implements DatabaseMetaData {

    private static final String DRIVER_NAME = "Vitess MariaDB JDBC Driver";
    private static Logger logger = Logger.getLogger(VitessMariaDBDatabaseMetadata.class.getName());

    public VitessMariaDBDatabaseMetadata(VitessConnection connection) throws SQLException {
        this.setConnection(connection);
    }

    public boolean nullsAreSortedAtStart() throws SQLException {
        return false;
    }

    public boolean nullsAreSortedAtEnd() throws SQLException {
        return !this.nullsAreSortedAtStart();
    }

    public String getDatabaseProductVersion() throws SQLException {
        throw new SQLFeatureNotSupportedException(
            Constants.SQLExceptionMessages.SQL_FEATURE_NOT_SUPPORTED);
    }

    public String getDriverName() throws SQLException {
        return DRIVER_NAME;
    }

    public String getSQLKeywords() throws SQLException {
        return "ACCESSIBLE," + "ANALYZE," + "ASENSITIVE," + "BEFORE," + "BIGINT," + "BINARY,"
            + "BLOB," + "CALL," +
            "CHANGE," + "CONDITION," + "DATABASE," + "DATABASES," + "DAY_HOUR," + "DAY_MICROSECOND,"
            + "DAY_MINUTE," + "DAY_SECOND," + "DELAYED," + "DETERMINISTIC," + "DISTINCTROW,"
            + "DIV," + "DUAL," + "EACH," + "ELSEIF," + "ENCLOSED," + "ESCAPED," + "EXIT,"
            + "EXPLAIN," + "FLOAT4," + "FLOAT8," + "FORCE," + "FULLTEXT," +
            "HIGH_PRIORITY," + "HOUR_MICROSECOND," + "HOUR_MINUTE," + "HOUR_SECOND," + "IF,"
            + "IGNORE," + "INFILE," + "INOUT," + "INT1," + "INT2," + "INT3," + "INT4," + "INT8,"
            + "ITERATE," + "KEY," + "KEYS," + "KILL," +
            "LEAVE," + "LIMIT," + "LINEAR," + "LINES," + "LOAD," + "LOCALTIME," + "LOCALTIMESTAMP,"
            + "LOCK," +
            "LONG," + "LONGBLOB," + "LONGTEXT," + "LOOP," + "LOW_PRIORITY," + "MEDIUMBLOB,"
            + "MEDIUMINT," +
            "MEDIUMTEXT," + "MIDDLEINT," + "MINUTE_MICROSECOND," + "MINUTE_SECOND," + "MOD,"
            + "MODIFIES," +
            "NO_WRITE_TO_BINLOG," + "OPTIMIZE," + "OPTIONALLY," + "OUT," + "OUTFILE," + "PURGE,"
            + "RANGE," + "READS," +
            "" + "READ_ONLY," + "READ_WRITE," + "REGEXP," + "RELEASE," + "RENAME," + "REPEAT,"
            + "REPLACE," +
            "REQUIRE," + "RETURN," + "RLIKE," + "SCHEMAS," + "SECOND_MICROSECOND," + "SENSITIVE,"
            + "SEPARATOR," +
            "SHOW," + "SPATIAL," + "SPECIFIC," + "SQLEXCEPTION," + "SQL_BIG_RESULT,"
            + "SQL_CALC_FOUND_ROWS," +
            "SQL_SMALL_RESULT," + "SSL," + "STARTING," + "STRAIGHT_JOIN," + "TERMINATED,"
            + "TINYBLOB," + "TINYINT," + "TINYTEXT," + "TRIGGER," + "UNDO," + "UNLOCK,"
            + "UNSIGNED," + "USE," + "UTC_DATE," + "UTC_TIME," +
            "UTC_TIMESTAMP," + "VARBINARY," + "VARCHARACTER," + "WHILE," + "X509," + "XOR,"
            + "YEAR_MONTH," +
            "ZEROFILL," + "GENERAL," + "IGNORE_SERVER_IDS," + "MASTER_HEARTBEAT_PERIOD,"
            + "MAXVALUE," + "RESIGNAL," + "SIGNAL" + "SLOW";
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

    public boolean supportsMultipleResultSets() throws SQLException {
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

    public boolean supportsSubqueriesInComparisons() throws SQLException {
        return false;
    }

    public int getMaxColumnsInTable() throws SQLException {
        return 512;
    }

    public int getMaxCursorNameLength() throws SQLException {
        return 0;
    }

    public int getMaxSchemaNameLength() throws SQLException {
        return 32;
    }

    public int getMaxCatalogNameLength() throws SQLException {
        return 0;
    }

    public int getMaxRowSize() throws SQLException {
        return 0;
    }

    public boolean doesMaxRowSizeIncludeBlobs() throws SQLException {
        return false;
    }

    public int getMaxStatementLength() throws SQLException {
        return 0;
    }

    public int getDefaultTransactionIsolation() throws SQLException {
        return this.connection.TRANSACTION_REPEATABLE_READ;
    }

    public boolean supportsTransactions() throws SQLException {
        return true;
    }

    public boolean supportsTransactionIsolationLevel(int level) throws SQLException {
        switch (level) {
            case Connection.TRANSACTION_READ_UNCOMMITTED:
            case Connection.TRANSACTION_READ_COMMITTED:
            case Connection.TRANSACTION_REPEATABLE_READ:
            case Connection.TRANSACTION_SERIALIZABLE:
                return true;
            default:
                return false;
        }
    }

    public ResultSet getTables(String catalog, String schemaPattern, String tableNamePattern,
        String[] types) throws SQLException {
        throw new SQLFeatureNotSupportedException(
            Constants.SQLExceptionMessages.SQL_FEATURE_NOT_SUPPORTED);
    }

    public ResultSet getSchemas() throws SQLException {
        throw new SQLFeatureNotSupportedException(
            Constants.SQLExceptionMessages.SQL_FEATURE_NOT_SUPPORTED);
    }

    public ResultSet getCatalogs() throws SQLException {
        throw new SQLFeatureNotSupportedException(
            Constants.SQLExceptionMessages.SQL_FEATURE_NOT_SUPPORTED);
    }

    public ResultSet getTableTypes() throws SQLException {
        throw new SQLFeatureNotSupportedException(
            Constants.SQLExceptionMessages.SQL_FEATURE_NOT_SUPPORTED);
    }

    public ResultSet getColumns(String catalog, String schemaPattern, String tableNamePattern,
        String columnNamePattern) throws SQLException {
        throw new SQLFeatureNotSupportedException(
            Constants.SQLExceptionMessages.SQL_FEATURE_NOT_SUPPORTED);
    }

    public ResultSet getColumnPrivileges(String catalog, String schema, String table,
        String columnNamePattern) throws SQLException {
        throw new SQLFeatureNotSupportedException(
            Constants.SQLExceptionMessages.SQL_FEATURE_NOT_SUPPORTED);
    }

    public ResultSet getTablePrivileges(String catalog, String schemaPattern,
        String tableNamePattern) throws SQLException {
        throw new SQLFeatureNotSupportedException(
            Constants.SQLExceptionMessages.SQL_FEATURE_NOT_SUPPORTED);
    }

    public ResultSet getBestRowIdentifier(String catalog, String schema, String table, int scope,
        boolean nullable) throws SQLException {
        throw new SQLFeatureNotSupportedException(
            Constants.SQLExceptionMessages.SQL_FEATURE_NOT_SUPPORTED);
    }

    public ResultSet getVersionColumns(String catalog, String schema, String table)
        throws SQLException {
        throw new SQLFeatureNotSupportedException(
            Constants.SQLExceptionMessages.SQL_FEATURE_NOT_SUPPORTED);
    }

    public ResultSet getPrimaryKeys(String catalog, String schema, String table)
        throws SQLException {
        throw new SQLFeatureNotSupportedException(
            Constants.SQLExceptionMessages.SQL_FEATURE_NOT_SUPPORTED);
    }

    public ResultSet getImportedKeys(String catalog, String schema, String table)
        throws SQLException {
        throw new SQLFeatureNotSupportedException(
            Constants.SQLExceptionMessages.SQL_FEATURE_NOT_SUPPORTED);
    }

    public ResultSet getExportedKeys(String catalog, String schema, String table)
        throws SQLException {
        throw new SQLFeatureNotSupportedException(
            Constants.SQLExceptionMessages.SQL_FEATURE_NOT_SUPPORTED);
    }

    public ResultSet getCrossReference(String parentCatalog, String parentSchema,
        String parentTable, String foreignCatalog, String foreignSchema, String foreignTable)
        throws SQLException {
        throw new SQLFeatureNotSupportedException(
            Constants.SQLExceptionMessages.SQL_FEATURE_NOT_SUPPORTED);
    }

    public ResultSet getTypeInfo() throws SQLException {
        String[] columnNames =
            {"TYPE_NAME", "DATA_TYPE", "PRECISION", "LITERAL_PREFIX", "LITERAL_SUFFIX",
                "CREATE_PARAMS", "NULLABLE", "CASE_SENSITIVE", "SEARCHABLE", "UNSIGNED_ATTRIBUTE",
                "FIXED_PREC_SCALE", "AUTO_INCREMENT", "LOCAL_TYPE_NAME", "MINIMUM_SCALE",
                "MAXIMUM_SCALE", "SQL_DATA_TYPE", "SQL_DATETIME_SUB", "NUM_PREC_RADIX"};
        Query.Type[] columnTypes =
            {Query.Type.VARCHAR, Query.Type.INT32, Query.Type.INT32, Query.Type.VARCHAR,
                Query.Type.VARCHAR, Query.Type.VARCHAR, Query.Type.INT32, Query.Type.BIT,
                Query.Type.INT16, Query.Type.BIT, Query.Type.BIT, Query.Type.BIT,
                Query.Type.VARCHAR, Query.Type.INT16, Query.Type.INT16, Query.Type.INT32,
                Query.Type.INT32, Query.Type.INT32};

        String[][] data =
            {{"BIT", "-7", "1", "", "", "", "1", "1", "3", "0", "0", "0", "BIT", "0", "0", "0", "0",
                "10"},
                {"BOOL", "-7", "1", "", "", "", "1", "1", "3", "0", "0", "0", "BOOL", "0", "0", "0",
                    "0", "10"},
                {"TINYINT", "-6", "3", "", "", "[(M)] [UNSIGNED] [ZEROFILL]", "1", "0", "3", "1",
                    "0", "1", "TINYINT", "0", "0", "0", "0", "10"},
                {"TINYINT UNSIGNED", "-6", "3", "", "", "[(M)] [UNSIGNED] [ZEROFILL]", "1", "0",
                    "3", "1", "0", "1", "TINYINT UNSIGNED", "0", "0", "0", "0", "10"},
                {"BIGINT", "-5", "19", "", "", "[(M)] [UNSIGNED] [ZEROFILL]", "1", "0", "3", "1",
                    "0", "1", "BIGINT", "0", "0", "0", "0", "10"},
                {"BIGINT UNSIGNED", "-5", "20", "", "", "[(M)] [ZEROFILL]", "1", "0", "3", "1", "0",
                    "1", "BIGINT UNSIGNED", "0", "0", "0", "0", "10"},
                {"LONG VARBINARY", "-4", "16777215", "'", "'", "", "1", "1", "3", "0", "0", "0",
                    "LONG VARBINARY", "0", "0", "0", "0", "10"},
                {"MEDIUMBLOB", "-4", "16777215", "'", "'", "", "1", "1", "3", "0", "0", "0",
                    "MEDIUMBLOB", "0", "0", "0", "0", "10"},
                {"LONGBLOB", "-4", "2147483647", "'", "'", "", "1", "1", "3", "0", "0", "0",
                    "LONGBLOB", "0", "0", "0", "0", "10"},
                {"BLOB", "-4", "65535", "'", "'", "", "1", "1", "3", "0", "0", "0", "BLOB", "0",
                    "0", "0", "0", "10"},
                {"TINYBLOB", "-4", "255", "'", "'", "", "1", "1", "3", "0", "0", "0", "TINYBLOB",
                    "0", "0", "0", "0", "10"},
                {"VARBINARY", "-3", "255", "'", "'", "(M)", "1", "1", "3", "0", "0", "0",
                    "VARBINARY", "0", "0", "0", "0", "10"},
                {"BINARY", "-2", "255", "'", "'", "(M)", "1", "1", "3", "0", "0", "0", "BINARY",
                    "0", "0", "0", "0", "10"},
                {"LONG VARCHAR", "-1", "16777215", "'", "'", "", "1", "0", "3", "0", "0", "0",
                    "LONG VARCHAR", "0", "0", "0", "0", "10"},
                {"MEDIUMTEXT", "-1", "16777215", "'", "'", "", "1", "0", "3", "0", "0", "0",
                    "MEDIUMTEXT", "0", "0", "0", "0", "10"},
                {"LONGTEXT", "-1", "2147483647", "'", "'", "", "1", "0", "3", "0", "0", "0",
                    "LONGTEXT", "0", "0", "0", "0", "10"},
                {"TEXT", "-1", "65535", "'", "'", "", "1", "0", "3", "0", "0", "0", "TEXT", "0",
                    "0", "0", "0", "10"},
                {"TINYTEXT", "-1", "255", "'", "'", "", "1", "0", "3", "0", "0", "0", "TINYTEXT",
                    "0", "0", "0", "0", "10"},
                {"CHAR", "1", "255", "'", "'", "(M)", "1", "0", "3", "0", "0", "0", "CHAR", "0",
                    "0", "0", "0", "10"},
                {"NUMERIC", "2", "65", "", "", "[(M,D])] [ZEROFILL]", "1", "0", "3", "0", "0", "1",
                    "NUMERIC", "-308", "308", "0", "0", "10"},
                {"DECIMAL", "3", "65", "", "", "[(M,D])] [ZEROFILL]", "1", "0", "3", "0", "0", "1",
                    "DECIMAL", "-308", "308", "0", "0", "10"},
                {"INTEGER", "4", "10", "", "", "[(M)] [UNSIGNED] [ZEROFILL]", "1", "0", "3", "1",
                    "0", "1", "INTEGER", "0", "0", "0", "0", "10"},
                {"INTEGER UNSIGNED", "4", "10", "", "", "[(M)] [ZEROFILL]", "1", "0", "3", "1", "0",
                    "1", "INTEGER UNSIGNED", "0", "0", "0", "0", "10"},
                {"INT", "4", "10", "", "", "[(M)] [UNSIGNED] [ZEROFILL]", "1", "0", "3", "1", "0",
                    "1", "INT", "0", "0", "0", "0", "10"},
                {"INT UNSIGNED", "4", "10", "", "", "[(M)] [ZEROFILL]", "1", "0", "3", "1", "0",
                    "1", "INT UNSIGNED", "0", "0", "0", "0", "10"},
                {"MEDIUMINT", "4", "7", "", "", "[(M)] [UNSIGNED] [ZEROFILL]", "1", "0", "3", "1",
                    "0", "1", "MEDIUMINT", "0", "0", "0", "0", "10"},
                {"MEDIUMINT UNSIGNED", "4", "8", "", "", "[(M)] [ZEROFILL]", "1", "0", "3", "1",
                    "0", "1", "MEDIUMINT UNSIGNED", "0", "0", "0", "0", "10"},
                {"SMALLINT", "5", "5", "", "", "[(M)] [UNSIGNED] [ZEROFILL]", "1", "0", "3", "1",
                    "0", "1", "SMALLINT", "0", "0", "0", "0", "10"},
                {"SMALLINT UNSIGNED", "5", "5", "", "", "[(M)] [ZEROFILL]", "1", "0", "3", "1", "0",
                    "1", "SMALLINT UNSIGNED", "0", "0", "0", "0", "10"},
                {"FLOAT", "7", "10", "", "", "[(M|D)] [ZEROFILL]", "1", "0", "3", "0", "0", "1",
                    "FLOAT", "-38", "38", "0", "0", "10"},
                {"DOUBLE", "8", "17", "", "", "[(M|D)] [ZEROFILL]", "1", "0", "3", "0", "0", "1",
                    "DOUBLE", "-308", "308", "0", "0", "10"},
                {"DOUBLE PRECISION", "8", "17", "", "", "[(M,D)] [ZEROFILL]", "1", "0", "3", "0",
                    "0", "1", "DOUBLE PRECISION", "-308", "308", "0", "0", "10"},
                {"REAL", "8", "17", "", "", "[(M,D)] [ZEROFILL]", "1", "0", "3", "0", "0", "1",
                    "REAL", "-308", "308", "0", "0", "10"},
                {"VARCHAR", "12", "255", "'", "'", "(M)", "1", "0", "3", "0", "0", "0", "VARCHAR",
                    "0", "0", "0", "0", "10"},
                {"ENUM", "12", "65535", "'", "'", "", "1", "0", "3", "0", "0", "0", "ENUM", "0",
                    "0", "0", "0", "10"},
                {"SET", "12", "64", "'", "'", "", "1", "0", "3", "0", "0", "0", "SET", "0", "0",
                    "0", "0", "10"},
                {"DATE", "91", "10", "'", "'", "", "1", "0", "3", "0", "0", "0", "DATE", "0", "0",
                    "0", "0", "10"},
                {"TIME", "92", "18", "'", "'", "[(M)]", "1", "0", "3", "0", "0", "0", "TIME", "0",
                    "0", "0", "0", "10"},
                {"DATETIME", "93", "27", "'", "'", "[(M)]", "1", "0", "3", "0", "0", "0",
                    "DATETIME", "0", "0", "0", "0", "10"},
                {"TIMESTAMP", "93", "27", "'", "'", "[(M)]", "1", "0", "3", "0", "0", "0",
                    "TIMESTAMP", "0", "0", "0", "0", "10"}};

        return new VitessResultSet(columnNames, columnTypes, data, this.connection);
    }

    public ResultSet getIndexInfo(String catalog, String schema, String table, boolean unique,
        boolean approximate) throws SQLException {
        throw new SQLFeatureNotSupportedException(
            Constants.SQLExceptionMessages.SQL_FEATURE_NOT_SUPPORTED);
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
        throw new SQLFeatureNotSupportedException(
            Constants.SQLExceptionMessages.SQL_FEATURE_NOT_SUPPORTED);
    }

    public VitessConnection getConnection() throws SQLException {
        return connection;
    }

    public ResultSet getSuperTypes(String catalog, String schemaPattern, String typeNamePattern)
        throws SQLException {
        throw new SQLFeatureNotSupportedException(
            Constants.SQLExceptionMessages.SQL_FEATURE_NOT_SUPPORTED);
    }

    public ResultSet getSuperTables(String catalog, String schemaPattern, String tableNamePattern)
        throws SQLException {
        throw new SQLFeatureNotSupportedException(
            Constants.SQLExceptionMessages.SQL_FEATURE_NOT_SUPPORTED);
    }

    public ResultSet getAttributes(String catalog, String schemaPattern, String typeNamePattern,
        String attributeNamePattern) throws SQLException {
        throw new SQLFeatureNotSupportedException(
            Constants.SQLExceptionMessages.SQL_FEATURE_NOT_SUPPORTED);
    }

    public int getSQLStateType() throws SQLException {
        return DatabaseMetaData.sqlStateSQL;
    }

    public boolean locatorsUpdateCopy() throws SQLException {
        return false;
    }

    public RowIdLifetime getRowIdLifetime() throws SQLException {
        throw new SQLFeatureNotSupportedException(
            Constants.SQLExceptionMessages.SQL_FEATURE_NOT_SUPPORTED);
    }

    public ResultSet getSchemas(String catalog, String schemaPattern) throws SQLException {
        throw new SQLFeatureNotSupportedException(
            Constants.SQLExceptionMessages.SQL_FEATURE_NOT_SUPPORTED);
    }

    public boolean autoCommitFailureClosesAllResultSets() throws SQLException {
        return false;
    }

    public ResultSet getClientInfoProperties() throws SQLException {
        throw new SQLFeatureNotSupportedException(
            Constants.SQLExceptionMessages.SQL_FEATURE_NOT_SUPPORTED);
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
        return connection.createStatement().executeQuery("SELECT ' ' TABLE_CAT, ' ' TABLE_SCHEM,"
            + "' ' TABLE_NAME, ' ' COLUMN_NAME, 0 DATA_TYPE, 0 COLUMN_SIZE, 0 DECIMAL_DIGITS,"
            + "10 NUM_PREC_RADIX, ' ' COLUMN_USAGE,  ' ' REMARKS, 0 CHAR_OCTET_LENGTH, 'YES' IS_NULLABLE FROM DUAL "
            + "WHERE 1=0");
    }

    public <T> T unwrap(Class<T> iface) throws SQLException {
        return null;
    }

    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return false;
    }
}
