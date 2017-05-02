package io.vitess.jdbc;

import io.vitess.client.Context;
import io.vitess.client.VTGateConn;
import io.vitess.client.VTGateTx;
import io.vitess.util.CommonUtils;
import io.vitess.util.Constants;
import io.vitess.util.MysqlDefs;
import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.ClientInfoStatus;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.logging.Logger;

/**
 * Created by harshit.gangal on 23/01/16.
 */
public class VitessConnection extends ConnectionProperties implements Connection {

    private static final int DEFAULT_RESULT_SET_TYPE = ResultSet.TYPE_FORWARD_ONLY;
    private static final int DEFAULT_RESULT_SET_CONCURRENCY = ResultSet.CONCUR_READ_ONLY;

    /* Get actual class name to be printed on */
    private static Logger logger = Logger.getLogger(VitessConnection.class.getName());
    private static DatabaseMetaData databaseMetaData = null;

    /**
     * A Map of currently open statements
     */
    private Set<Statement> openStatements = new HashSet<>();
    private VitessVTGateManager.VTGateConnections vTGateConnections;
    private VTGateTx vtGateTx;
    private boolean closed = true;
    private boolean autoCommit = true;
    private boolean readOnly = false;
    private DBProperties dbProperties;
    private final VitessJDBCUrl vitessJDBCUrl;

    /**
     * Constructor to Create Connection Object
     *
     * @param url  - Connection url
     * @param connectionProperties - property for the connection
     * @throws SQLException
     */
    public VitessConnection(String url, Properties connectionProperties) throws SQLException {
        try {
            this.vitessJDBCUrl = new VitessJDBCUrl(url, connectionProperties);
            this.closed = false;
            this.dbProperties = null;
        } catch (Exception e) {
            throw new SQLException(
                Constants.SQLExceptionMessages.CONN_INIT_ERROR + " - " + e.getMessage(), e);
        }

        initializeProperties(vitessJDBCUrl.getProperties());
    }

    public void connect() {
        this.vTGateConnections = new VitessVTGateManager.VTGateConnections(this);
    }

    /**
     * Creates statement for the given connection
     *
     * @return Statement Object
     * @throws SQLException
     */
    public Statement createStatement() throws SQLException {
        checkOpen();

        return new VitessStatement(this);
    }

    /**
     * Create PreparedStatement for the given connection & sql
     *
     * @param sql - Sql Statement
     * @return PreparedStatement Object
     * @throws SQLException
     */
    public PreparedStatement prepareStatement(String sql) throws SQLException {
        checkOpen();
        return new VitessPreparedStatement(this, sql);
    }

    /**
     * This method returns the sql form which the driver will sent to database
     *
     * @param sql - Sql Statement
     * @return Form of the sql that the driver will sent to the underlying database
     * @throws SQLException
     */
    public String nativeSQL(String sql) throws SQLException {
        checkOpen();
        return sql;
    }

    /**
     * Return Auto commit status
     *
     * @return autoCommit
     * @throws SQLException
     */
    public boolean getAutoCommit() throws SQLException {
        checkOpen();
        return this.autoCommit;
    }

    /**
     * Sets this connection's auto-commit mode to the given state.
     *
     * @param autoCommit - true or false
     * @throws SQLException
     */
    public void setAutoCommit(boolean autoCommit) throws SQLException {
        checkOpen();
        if (this.autoCommit != autoCommit) { //If same then no-op
            //Old Transaction Needs to be committed as per JDBC 4.1 Spec.
            if (isInTransaction()) {
                this.commit();
            }
            this.autoCommit = autoCommit;
        }
    }

    /**
     * Commit on existing transaction and closed the transaction
     *
     * @throws SQLException
     */
    public void commit() throws SQLException {
        checkOpen();
        checkAutoCommit(Constants.SQLExceptionMessages.COMMIT_WHEN_AUTO_COMMIT_TRUE);
        try {
            if (isInTransaction()) {
                Context context = createContext(Constants.CONNECTION_TIMEOUT);
                this.vtGateTx.commit(context, getTwopcEnabled()).checkedGet();
            }
        } finally {
            this.vtGateTx = null;
        }

    }

    /**
     * Rollback on existing transaction and closed the transaction
     *
     * @throws SQLException
     */
    public void rollback() throws SQLException {
        checkOpen();
        checkAutoCommit(Constants.SQLExceptionMessages.ROLLBACK_WHEN_AUTO_COMMIT_TRUE);
        try {
            if (isInTransaction()) {
                Context context = createContext(Constants.CONNECTION_TIMEOUT);
                this.vtGateTx.rollback(context).checkedGet();
            }
        } finally {
            this.vtGateTx = null;
        }
    }

    /**
     * Closes an existing connection
     *
     * @throws SQLException
     */
    public void close() throws SQLException {
        if (!this.closed) { //no-op when Connection already closed
            try {
                if (isInTransaction()) { //Rolling back active transaction on close
                    this.rollback();
                }
                closeAllOpenStatements();
            } finally {
                this.vtGateTx = null;
                this.closed = true;
            }
        }
    }

    /**
     * Return Connection state
     *
     * @return DatabaseMetadata Object
     * @throws SQLException
     */
    public boolean isClosed() throws SQLException {
        return this.closed;
    }

    public DatabaseMetaData getMetaData() throws SQLException {
        checkOpen();
        if (!metadataNullOrClosed()) {
            return databaseMetaData;
        } else {
            synchronized (VitessConnection.class) {
                if (metadataNullOrClosed()) {
                    String dbEngine = initializeDBProperties();
                    if (dbEngine.equals("mariadb")) {
                        databaseMetaData = new VitessMariaDBDatabaseMetadata(this);
                    } else {
                        databaseMetaData = new VitessMySQLDatabaseMetadata(this);
                    }
                }
            }
            return databaseMetaData;
        }
    }

    private boolean metadataNullOrClosed() throws SQLException {
        return null == databaseMetaData || null == databaseMetaData.getConnection() || databaseMetaData.getConnection().isClosed();
    }

    public boolean isReadOnly() throws SQLException {
        checkOpen();
        return readOnly;
    }

    /**
     * Set ReadOnly for the connection
     *
     * @param readOnly - true or false
     * @throws SQLException
     */
    public void setReadOnly(boolean readOnly) throws SQLException {
        checkOpen();

        if (isInTransaction()) {
            throw new SQLException(
                Constants.SQLExceptionMessages.METHOD_CALLED_ON_OPEN_TRANSACTION);
        }

        if (readOnly) {
            throw new SQLFeatureNotSupportedException(Constants.SQLExceptionMessages.READ_ONLY);
        }
        this.readOnly = false;
    }

    /**
     * Return Catalog
     *
     * @return catalog string
     * @throws SQLException
     */
    public String getCatalog() throws SQLException {
        checkOpen();
        return this.vitessJDBCUrl.getCatalog();
    }

    /**
     * As per JDBC 4.1 specs, if database does not support catalog, then silently ignore the call
     *
     * @param catalog - Catalog value
     * @throws SQLException
     */
    public void setCatalog(String catalog) throws SQLException {
        checkOpen();
        this.vitessJDBCUrl.setCatalog(catalog); //Ignoring any affect
    }

    /**
     * Get the Isolation Level
     *
     * @return Isolation Level of the Database
     * @throws SQLException
     */
    public int getTransactionIsolation() throws SQLException {
        checkOpen();
        return this.getMetaData().getDefaultTransactionIsolation();
    }

    /**
     * TODO: Currently it will not allow to change the isolation level
     *
     * @param level - Isolation Level
     * @throws SQLException
     */
    public void setTransactionIsolation(int level) throws SQLException {
        /* Future Implementation of this method
        checkOpen();
        if (null != this.vtGateTx) {
            try {
                this.vtGateTx.rollback(this.context);
            } catch (SQLException ex) {
                throw new SQLException(ex);
            } finally {
                this.vtGateTx = null;
            }
        }
        if (Connection.TRANSACTION_NONE == level || !getMetaData()
            .supportsTransactionIsolationLevel(level)) {
            throw new SQLException(Constants.SQLExceptionMessages.ISOLATION_LEVEL_NOT_SUPPORTED);
        } */
        throw new SQLFeatureNotSupportedException(
            Constants.SQLExceptionMessages.SQL_FEATURE_NOT_SUPPORTED);
    }

    /**
     * Return Warnings
     * <p/>
     * TODO: Not implementing as Error is Thrown when occured
     *
     * @return SQLWarning or null
     * @throws SQLException
     */
    public SQLWarning getWarnings() throws SQLException {
        checkOpen();
        return null;
    }

    /**
     * Clear the warnings - Not saving Warnings
     *
     * @throws SQLException
     */
    public void clearWarnings() throws SQLException {
        checkOpen();
    }

    /**
     * Create Statement object with ResultSet properties
     *
     * @param resultSetType        - ResultSet Type
     * @param resultSetConcurrency - ResultSet Concurrency
     * @return Statement Object
     * @throws SQLException
     */
    public Statement createStatement(int resultSetType, int resultSetConcurrency)
        throws SQLException {
        VitessStatement vitessStatement;

        checkOpen();
        if (resultSetType != ResultSet.TYPE_FORWARD_ONLY) {
            throw new SQLException(Constants.SQLExceptionMessages.RESULT_SET_TYPE_NOT_SUPPORTED);
        }
        if (resultSetConcurrency != ResultSet.CONCUR_READ_ONLY) {
            throw new SQLException(Constants.SQLExceptionMessages.RESULT_SET_CONCUR_NOT_SUPPORTED);
        }
        vitessStatement = new VitessStatement(this, resultSetType, resultSetConcurrency);

        return vitessStatement;
    }

    /**
     * Create PreparedStatement object with ResultSet properties
     *
     * @param sql                  - Sql Statement
     * @param resultSetType        - ResultSet Type
     * @param resultSetConcurrency - ResultSet Concurrency
     * @return PreparedStatement Object
     * @throws SQLException
     */
    public PreparedStatement prepareStatement(String sql, int resultSetType,
        int resultSetConcurrency) throws SQLException {
        VitessPreparedStatement vitessPreparedStatement;

        checkOpen();
        if (resultSetType != ResultSet.TYPE_FORWARD_ONLY) {
            throw new SQLException(Constants.SQLExceptionMessages.RESULT_SET_TYPE_NOT_SUPPORTED);
        }
        if (resultSetConcurrency != ResultSet.CONCUR_READ_ONLY) {
            throw new SQLException(Constants.SQLExceptionMessages.RESULT_SET_CONCUR_NOT_SUPPORTED);
        }
        vitessPreparedStatement =
            new VitessPreparedStatement(this, sql, resultSetType, resultSetConcurrency);

        return vitessPreparedStatement;
    }

    /**
     * Return ResultSet Holdability
     *
     * @return ResetSet Holdability Type
     * @throws SQLException
     */
    public int getHoldability() throws SQLException {
        checkOpen();
        return this.getMetaData().getResultSetHoldability();
    }

    /**
     * Feature is not Supported
     *
     * @param holdability - ResetSet Holdability Type
     * @throws SQLException
     */
    public void setHoldability(int holdability) throws SQLException {
        throw new SQLFeatureNotSupportedException(
            Constants.SQLExceptionMessages.SQL_FEATURE_NOT_SUPPORTED);
    }

    /**
     * TODO : This method should actually validate the connection.
     *
     * @param timeout
     * @return
     * @throws SQLException
     */
    public boolean isValid(int timeout) throws SQLException {
        if (timeout < 0) {
            throw new SQLException(Constants.SQLExceptionMessages.TIMEOUT_NEGATIVE);
        }
        return closed ? Boolean.FALSE : Boolean.TRUE;
    }

    /**
     * TODO: For Implementation Possibility
     *
     * @param name  - Property Name
     * @param value - Property Value
     * @throws SQLClientInfoException
     */
    public void setClientInfo(String name, String value) throws SQLClientInfoException {
        Map<String, ClientInfoStatus> errorMap = new HashMap<>();
        ClientInfoStatus clientInfoStatus = ClientInfoStatus.REASON_UNKNOWN;
        errorMap.put(name, clientInfoStatus);

        throw new SQLClientInfoException(Constants.SQLExceptionMessages.SQL_FEATURE_NOT_SUPPORTED,
            errorMap);
    }

    /**
     * TODO: For Implementation Possibility
     *
     * @param name - Property Name
     * @return Property Value
     * @throws SQLException
     */
    public String getClientInfo(String name) throws SQLException {
        return null;
    }

    /**
     * TODO: For Implementation Possibility
     *
     * @return - Property Object
     * @throws SQLException
     */
    public Properties getClientInfo() throws SQLException {
        return null;
    }

    /**
     * TODO: For Implementation Possibility
     *
     * @param properties - Property Object
     * @throws SQLClientInfoException
     */
    public void setClientInfo(Properties properties) throws SQLClientInfoException {
        Map<String, ClientInfoStatus> errorMap = new HashMap<>();
        ClientInfoStatus clientInfoStatus = ClientInfoStatus.REASON_UNKNOWN;
        for (String name : properties.stringPropertyNames()) {
            errorMap.put(name, clientInfoStatus);
        }

        throw new SQLClientInfoException(Constants.SQLExceptionMessages.SQL_FEATURE_NOT_SUPPORTED,
            errorMap);
    }

    /**
     * No-op for now
     *
     * @return Schema
     * @throws SQLException
     */
    public String getSchema() throws SQLException {
        checkOpen();
        return null;
    }

    /**
     * No-op for now
     *
     * @param schema - Schema
     * @throws SQLException
     */
    public void setSchema(String schema) throws SQLException {
        checkOpen();
    }

    /**
     * Abort the Connection
     *
     * @param executor - Executor
     * @throws SQLException
     */
    public void abort(Executor executor) throws SQLException {
        if (!closed) { //no-op on closed
            if (null == executor) {
                throw new SQLException(Constants.SQLExceptionMessages.EXECUTOR_NULL);
            }

            executor.execute(new Runnable() {
                @Override public void run() {
                    try {
                        close();
                    } catch (SQLException e) {
                        throw new RuntimeException(e);
                    }
                }
            });
        }
    }

    /**
     * Unwrap a class
     *
     * @param iface - A Class defining an interface that the result must implement.
     * @param <T>   - the type of the class modeled by this Class object
     * @return an object that implements the interface. May be a proxy for the actual implementing object.
     * @throws SQLException
     */
    public <T> T unwrap(Class<T> iface) throws SQLException {
        try {
            return iface.cast(this);
        } catch (ClassCastException cce) {
            throw new SQLException(
                Constants.SQLExceptionMessages.CLASS_CAST_EXCEPTION + iface.toString(), cce);
        }
    }

    /**
     * Checking Wrapper
     *
     * @param iface - A Class defining an interface that the result must implement.
     * @return true if this implements the interface or directly or indirectly wraps an object that does.
     * @throws SQLException
     */
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        checkOpen();
        return iface.isInstance(this);
    }

    public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys)
        throws SQLException {
        checkOpen();
        return new VitessPreparedStatement(this, sql, autoGeneratedKeys);
    }

    //Methods created for this class

    private void checkOpen() throws SQLException {
        if (this.closed) {
            throw new SQLException(Constants.SQLExceptionMessages.CONN_CLOSED);
        }
    }

    private void checkAutoCommit(String exception) throws SQLException {
        if (this.autoCommit) {
            throw new SQLException(exception);
        }
    }

    private boolean isInTransaction() {
        return null != this.vtGateTx;
    }

    public VTGateConn getVtGateConn() {
        return vTGateConnections.getVtGateConnInstance();
    }

    public VTGateTx getVtGateTx() {
        return vtGateTx;
    }

    public void setVtGateTx(VTGateTx vtGateTx) {
        this.vtGateTx = vtGateTx;
    }

    public VitessJDBCUrl getUrl() {
        return this.vitessJDBCUrl;
    }

    /**
     * Register a Statement instance as open.
     *
     * @param statement the Statement instance to remove
     */
    public void registerStatement(Statement statement) {
        this.openStatements.add(statement);
    }

    /**
     * Remove the given statement from the list of open statements
     *
     * @param statement the Statement instance to remove
     */
    public void unregisterStatement(Statement statement) {
        this.openStatements.remove(statement);
    }

    /**
     * Closes all currently open statements.
     *
     * @throws SQLException
     */
    private void closeAllOpenStatements() throws SQLException {
        SQLException postponedException = null;

        // Copy openStatements, since VitessStatement.close()
        // deregisters itself, modifying the original set.
        for (Statement statement : new ArrayList<>(this.openStatements)) {
            try {
                VitessStatement vitessStatement = (VitessStatement) statement;
                vitessStatement.close();
            } catch (SQLException sqlEx) {
                postponedException = sqlEx; // throw it later, cleanup all statements first
            }
        }

        this.openStatements.clear();

        if (postponedException != null) {
            throw postponedException;
        }

    }

    /**
     * @return keyspace name
     */
    public String getKeyspace() {
        return this.vitessJDBCUrl.getKeyspace();
    }

    // UnSupported Feature List

    /**
     * TODO: To support Stored Procedure
     *
     * @param sql
     * @return
     * @throws SQLException
     */
    public CallableStatement prepareCall(String sql) throws SQLException {
        throw new SQLFeatureNotSupportedException(
            Constants.SQLExceptionMessages.SQL_FEATURE_NOT_SUPPORTED);
    }

    /**
     * TODO: To support Stored Procedure
     *
     * @param sql
     * @param resultSetType
     * @param resultSetConcurrency
     * @return
     * @throws SQLException
     */
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency)
        throws SQLException {
        throw new SQLFeatureNotSupportedException(
            Constants.SQLExceptionMessages.SQL_FEATURE_NOT_SUPPORTED);
    }

    public Map<String, Class<?>> getTypeMap() throws SQLException {
        throw new SQLFeatureNotSupportedException(
            Constants.SQLExceptionMessages.SQL_FEATURE_NOT_SUPPORTED);
    }

    public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
        throw new SQLFeatureNotSupportedException(
            Constants.SQLExceptionMessages.SQL_FEATURE_NOT_SUPPORTED);
    }

    public Savepoint setSavepoint() throws SQLException {
        throw new SQLFeatureNotSupportedException(
            Constants.SQLExceptionMessages.SQL_FEATURE_NOT_SUPPORTED);
    }

    public Savepoint setSavepoint(String name) throws SQLException {
        throw new SQLFeatureNotSupportedException(
            Constants.SQLExceptionMessages.SQL_FEATURE_NOT_SUPPORTED);
    }

    public void rollback(Savepoint savepoint) throws SQLException {
        throw new SQLFeatureNotSupportedException(
            Constants.SQLExceptionMessages.SQL_FEATURE_NOT_SUPPORTED);
    }

    public void releaseSavepoint(Savepoint savepoint) throws SQLException {
        throw new SQLFeatureNotSupportedException(
            Constants.SQLExceptionMessages.SQL_FEATURE_NOT_SUPPORTED);
    }

    public Statement createStatement(int resultSetType, int resultSetConcurrency,
        int resultSetHoldability) throws SQLException {
        throw new SQLFeatureNotSupportedException(
            Constants.SQLExceptionMessages.SQL_FEATURE_NOT_SUPPORTED);
    }

    public PreparedStatement prepareStatement(String sql, int resultSetType,
        int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        throw new SQLFeatureNotSupportedException(
            Constants.SQLExceptionMessages.SQL_FEATURE_NOT_SUPPORTED);
    }

    /**
     * TODO: To support Stored Procedure
     *
     * @param sql
     * @param resultSetType
     * @param resultSetConcurrency
     * @param resultSetHoldability
     * @return
     * @throws SQLException
     */
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency,
        int resultSetHoldability) throws SQLException {
        throw new SQLFeatureNotSupportedException(
            Constants.SQLExceptionMessages.SQL_FEATURE_NOT_SUPPORTED);
    }

    public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
        throw new SQLFeatureNotSupportedException(
            Constants.SQLExceptionMessages.SQL_FEATURE_NOT_SUPPORTED);
    }

    public PreparedStatement prepareStatement(String sql, String[] columnNames)
        throws SQLException {
        throw new SQLFeatureNotSupportedException(
            Constants.SQLExceptionMessages.SQL_FEATURE_NOT_SUPPORTED);
    }

    public Clob createClob() throws SQLException {
        throw new SQLFeatureNotSupportedException(
            Constants.SQLExceptionMessages.SQL_FEATURE_NOT_SUPPORTED);
    }

    public Blob createBlob() throws SQLException {
        throw new SQLFeatureNotSupportedException(
            Constants.SQLExceptionMessages.SQL_FEATURE_NOT_SUPPORTED);
    }

    public NClob createNClob() throws SQLException {
        throw new SQLFeatureNotSupportedException(
            Constants.SQLExceptionMessages.SQL_FEATURE_NOT_SUPPORTED);
    }

    public SQLXML createSQLXML() throws SQLException {
        throw new SQLFeatureNotSupportedException(
            Constants.SQLExceptionMessages.SQL_FEATURE_NOT_SUPPORTED);
    }

    public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
        throw new SQLFeatureNotSupportedException(
            Constants.SQLExceptionMessages.SQL_FEATURE_NOT_SUPPORTED);
    }

    public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
        throw new SQLFeatureNotSupportedException(
            Constants.SQLExceptionMessages.SQL_FEATURE_NOT_SUPPORTED);
    }

    public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
        throw new SQLFeatureNotSupportedException(
            Constants.SQLExceptionMessages.SQL_FEATURE_NOT_SUPPORTED);
    }

    public int getNetworkTimeout() throws SQLException {
        throw new SQLFeatureNotSupportedException(
            Constants.SQLExceptionMessages.SQL_FEATURE_NOT_SUPPORTED);
    }

    private String initializeDBProperties() throws SQLException {
        HashMap<String, String> dbVariables = new HashMap<>();
        String dbEngine = null;

        if (metadataNullOrClosed()) {
            String versionValue;
            ResultSet resultSet = null;
            VitessStatement vitessStatement = new VitessStatement(this);
            try {
                resultSet = vitessStatement.executeQuery(
                    "SHOW VARIABLES WHERE VARIABLE_NAME IN (\'tx_isolation\',\'INNODB_VERSION\', \'lower_case_table_names\')");
                while (resultSet.next()) {
                    dbVariables.put(resultSet.getString(1), resultSet.getString(2));
                }
                versionValue = dbVariables.get("innodb_version");
                String transactionIsolation = dbVariables.get("tx_isolation");
                String lowerCaseTables = dbVariables.get("lower_case_table_names");
                String productVersion = "";
                String majorVersion = "";
                String minorVersion = "";
                int isolationLevel = 0;
                if (MysqlDefs.mysqlConnectionTransactionMapping.containsKey(transactionIsolation)) {
                    isolationLevel =
                        MysqlDefs.mysqlConnectionTransactionMapping.get(transactionIsolation);
                }
                if (null != versionValue) {
                    if (versionValue.toLowerCase().contains("mariadb")) {
                        dbEngine = "mariadb";
                    } else {
                        dbEngine = "mysql";
                    }
                    if (versionValue.contains("-")) {
                        String[] versions = versionValue.split("-");
                        productVersion = versions[0];
                    } else {
                        productVersion = versionValue;
                    }
                    String[] dbVersions = productVersion.split("\\.", 3);
                    majorVersion = dbVersions[0];
                    minorVersion = dbVersions[1];
                }
                this.dbProperties =
                    new DBProperties(productVersion, majorVersion, minorVersion, isolationLevel, lowerCaseTables);
            } finally {
                if (null != resultSet) {
                    resultSet.close();
                }
                vitessStatement.close();
            }

        }
        return dbEngine;
    }

    public DBProperties getDbProperties() {
        return this.dbProperties;
    }

    public Context createContext(long deadlineAfter) {
        return CommonUtils.createContext(this.vitessJDBCUrl.getUsername(), deadlineAfter);
    }

    public String getUsername() {
        return this.vitessJDBCUrl.getUsername();
    }
}
