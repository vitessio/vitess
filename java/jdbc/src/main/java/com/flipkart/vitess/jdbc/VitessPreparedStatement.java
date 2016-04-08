package com.flipkart.vitess.jdbc;

import com.flipkart.vitess.util.Constants;
import com.flipkart.vitess.util.StringUtils;
import com.youtube.vitess.client.Context;
import com.youtube.vitess.client.VTGateConn;
import com.youtube.vitess.client.VTGateTx;
import com.youtube.vitess.client.cursor.Cursor;
import com.youtube.vitess.mysql.DateTime;
import com.youtube.vitess.proto.Topodata;

import java.io.InputStream;
import java.io.Reader;
import java.io.StringReader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.*;
import java.sql.Date;
import java.text.DateFormat;
import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.logging.Logger;

/**
 * Created by harshit.gangal on 25/01/16.
 */
public class VitessPreparedStatement extends VitessStatement implements PreparedStatement {

    /* Get actual class name to be printed on */
    private static Logger logger = Logger.getLogger(VitessPreparedStatement.class.getName());
    private final String sql;
    private final Map<String, Object> bindVariables;

    public VitessPreparedStatement(VitessConnection vitessConnection, String sql)
        throws SQLException {
        this(vitessConnection, sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
    }

    public VitessPreparedStatement(VitessConnection vitessConnection, String sql, int resultSetType,
        int resultSetConcurrency) throws SQLException {
        super(vitessConnection, resultSetType, resultSetConcurrency);
        checkSQLNullOrEmpty(sql);
        this.bindVariables = new HashMap<>();
        this.sql = sql;
    }

    public ResultSet executeQuery() throws SQLException {
        VTGateConn vtGateConn;
        Topodata.TabletType tabletType;
        Cursor cursor;
        boolean showSql;

        checkOpen();
        closeOpenResultSetAndResetCount();

        vtGateConn = this.vitessConnection.getVtGateConn();
        tabletType = this.vitessConnection.getTabletType();

        showSql = StringUtils.startsWithIgnoreCaseAndWs(sql, Constants.SQL_SHOW);
        if (showSql) {
            String keyspace = this.vitessConnection.getKeyspace();
            List<byte[]> keyspaceIds = Arrays.asList(new byte[] {1}); //To Hit any single shard

            Context context = this.vitessConnection.createContext(this.queryTimeoutInMillis);
            cursor = vtGateConn
                .executeKeyspaceIds(context, this.sql, keyspace, keyspaceIds, null, tabletType)
                .checkedGet();
        } else {
            if (tabletType != Topodata.TabletType.MASTER || this.vitessConnection.getAutoCommit()) {
                Context context = this.vitessConnection.createContext(this.queryTimeoutInMillis);
                cursor = vtGateConn.execute(context, this.sql, this.bindVariables, tabletType)
                    .checkedGet();
            } else {
                VTGateTx vtGateTx = this.vitessConnection.getVtGateTx();
                if (vtGateTx == null) {
                    Context context =
                        this.vitessConnection.createContext(this.queryTimeoutInMillis);
                    vtGateTx = vtGateConn.begin(context).checkedGet();
                    this.vitessConnection.setVtGateTx(vtGateTx);
                }
                Context context = this.vitessConnection.createContext(this.queryTimeoutInMillis);
                cursor = vtGateTx.execute(context, this.sql, this.bindVariables, tabletType)
                    .checkedGet();
            }
        }

        if (null == cursor) {
            throw new SQLException(Constants.SQLExceptionMessages.METHOD_CALL_FAILED);
        }

        this.vitessResultSet = new VitessResultSet(cursor, this);
        return (this.vitessResultSet);
    }

    public int executeUpdate() throws SQLException {
        VTGateConn vtGateConn;
        Topodata.TabletType tabletType;
        Cursor cursor;

        checkOpen();
        closeOpenResultSetAndResetCount();

        vtGateConn = this.vitessConnection.getVtGateConn();
        tabletType = this.vitessConnection.getTabletType();

        if (tabletType != Topodata.TabletType.MASTER) {
            throw new SQLException(Constants.SQLExceptionMessages.DML_NOT_ON_MASTER);
        }

        VTGateTx vtGateTx = this.vitessConnection.getVtGateTx();
        if (vtGateTx == null) {
            Context context = this.vitessConnection.createContext(this.queryTimeoutInMillis);
            vtGateTx = vtGateConn.begin(context).checkedGet();
            this.vitessConnection.setVtGateTx(vtGateTx);
        }

        if (this.vitessConnection.getAutoCommit()) {
            Context context = this.vitessConnection.createContext(this.queryTimeoutInMillis);
            cursor =
                vtGateTx.execute(context, this.sql, this.bindVariables, tabletType).checkedGet();
            vtGateTx.commit(context).checkedGet();
            this.vitessConnection.setVtGateTx(null);
        } else {
            Context context = this.vitessConnection.createContext(this.queryTimeoutInMillis);
            cursor =
                vtGateTx.execute(context, this.sql, this.bindVariables, tabletType).checkedGet();
        }

        if (null == cursor) {
            throw new SQLException(Constants.SQLExceptionMessages.METHOD_CALL_FAILED);
        }

        if (!(null == cursor.getFields() || cursor.getFields().isEmpty())) {
            throw new SQLException(Constants.SQLExceptionMessages.SQL_RETURNED_RESULT_SET);
        }

        this.resultCount = cursor.getRowsAffected();

        int truncatedUpdateCount;

        if (this.resultCount > Integer.MAX_VALUE) {
            truncatedUpdateCount = Integer.MAX_VALUE;
        } else {
            truncatedUpdateCount = (int) this.resultCount;
        }
        return truncatedUpdateCount;
    }

    public boolean execute() throws SQLException {
        VTGateConn vtGateConn;
        Topodata.TabletType tabletType;
        Cursor cursor;
        boolean selectSql;
        boolean showSql;

        checkOpen();
        closeOpenResultSetAndResetCount();

        vtGateConn = this.vitessConnection.getVtGateConn();
        tabletType = this.vitessConnection.getTabletType();

        selectSql = StringUtils.startsWithIgnoreCaseAndWs(this.sql, Constants.SQL_SELECT);
        showSql = StringUtils.startsWithIgnoreCaseAndWs(sql, Constants.SQL_SHOW);

        if (showSql) {
            String keyspace = this.vitessConnection.getKeyspace();

            //To Hit any single shard
            List<byte[]> keyspaceIds = Arrays.asList(new byte[] {1});

            Context context = this.vitessConnection.createContext(this.queryTimeoutInMillis);
            cursor = vtGateConn
                .executeKeyspaceIds(context, this.sql, keyspace, keyspaceIds, null, tabletType)
                .checkedGet();
            if (!(null == cursor || null == cursor.getFields() || cursor.getFields().isEmpty())) {
                this.vitessResultSet = new VitessResultSet(cursor, this);
                return true;
            }
            throw new SQLException(Constants.SQLExceptionMessages.METHOD_CALL_FAILED);
        } else if (selectSql) {
            this.executeQuery();
            return true;
        } else {
            this.executeUpdate();
            return false;
        }
    }

    public void clearParameters() throws SQLException {
        checkOpen();
        this.bindVariables.clear();
    }

    public void setNull(int parameterIndex, int sqlType) throws SQLException {
        checkOpen();
        this.bindVariables.put(Constants.LITERAL_V + parameterIndex, null);
    }

    public void setBoolean(int parameterIndex, boolean x) throws SQLException {
        checkOpen();
        this.bindVariables.put(Constants.LITERAL_V + parameterIndex, x);
    }

    public void setByte(int parameterIndex, byte x) throws SQLException {
        checkOpen();
        this.bindVariables.put(Constants.LITERAL_V + parameterIndex, x);
    }

    public void setShort(int parameterIndex, short x) throws SQLException {
        checkOpen();
        this.bindVariables.put(Constants.LITERAL_V + parameterIndex, x);
    }

    public void setInt(int parameterIndex, int x) throws SQLException {
        checkOpen();
        this.bindVariables.put(Constants.LITERAL_V + parameterIndex, x);
    }

    public void setLong(int parameterIndex, long x) throws SQLException {
        checkOpen();
        this.bindVariables.put(Constants.LITERAL_V + parameterIndex, x);
    }

    public void setFloat(int parameterIndex, float x) throws SQLException {
        checkOpen();
        this.bindVariables.put(Constants.LITERAL_V + parameterIndex, x);
    }

    public void setDouble(int parameterIndex, double x) throws SQLException {
        checkOpen();
        this.bindVariables.put(Constants.LITERAL_V + parameterIndex, x);
    }

    public void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException {
        checkOpen();
        this.bindVariables.put(Constants.LITERAL_V + parameterIndex, x);
    }

    public void setString(int parameterIndex, String x) throws SQLException {
        checkOpen();
        this.bindVariables.put(Constants.LITERAL_V + parameterIndex, x);
    }

    public void setBytes(int parameterIndex, byte[] x) throws SQLException {
        checkOpen();
        this.bindVariables.put(Constants.LITERAL_V + parameterIndex, x);
    }

    public void setDate(int parameterIndex, Date x) throws SQLException {
        checkOpen();
        String date = DateTime.formatDate(x);
        this.bindVariables.put(Constants.LITERAL_V + parameterIndex, date);
    }

    public void setTime(int parameterIndex, Time x) throws SQLException {
        checkOpen();
        String time = DateTime.formatTime(x);
        this.bindVariables.put(Constants.LITERAL_V + parameterIndex, time);
    }

    public void setTimestamp(int parameterIndex, Timestamp x) throws SQLException {
        checkOpen();
        String timeStamp = DateTime.formatTimestamp(x);
        this.bindVariables.put(Constants.LITERAL_V + parameterIndex, timeStamp);
    }

    public void setDate(int parameterIndex, Date x, Calendar cal) throws SQLException {
        checkOpen();
        String date = DateTime.formatDate(x, cal);
        this.bindVariables.put(Constants.LITERAL_V + parameterIndex, date);
    }

    public void setTime(int parameterIndex, Time x, Calendar cal) throws SQLException {
        checkOpen();
        String time = DateTime.formatTime(x, cal);
        this.bindVariables.put(Constants.LITERAL_V + parameterIndex, time);
    }

    public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal) throws SQLException {
        checkOpen();
        String timeStamp = DateTime.formatTimestamp(x, cal);
        this.bindVariables.put(Constants.LITERAL_V + parameterIndex, timeStamp);
    }

    public void setObject(int parameterIndex, Object x) throws SQLException {
        if (x == null) {
            setNull(parameterIndex, Types.NULL);
        } else if (x instanceof String) {
            setString(parameterIndex, (String) x);
        } else if (x instanceof Short) {
            setShort(parameterIndex, (Short) x);
        } else if (x instanceof Integer) {
            setInt(parameterIndex, (Integer) x);
        } else if (x instanceof Long) {
            setLong(parameterIndex, (Long) x);
        } else if (x instanceof Float) {
            setFloat(parameterIndex, (Float) x);
        } else if (x instanceof Double) {
            setDouble(parameterIndex, (Double) x);
        } else if (x instanceof Boolean) {
            setBoolean(parameterIndex, (Boolean) x);
        } else if (x instanceof Byte) {
            setByte(parameterIndex, (Byte) x);
        } else if (x instanceof Character) {
            setString(parameterIndex, String.valueOf(x));
        } else if (x instanceof Date) {
            setDate(parameterIndex, (Date) x);
        } else if (x instanceof Time) {
            setTime(parameterIndex, (Time) x);
        } else if (x instanceof Timestamp) {
            setTimestamp(parameterIndex, (Timestamp) x);
        } else if (x instanceof BigDecimal) {
            setBigDecimal(parameterIndex, (BigDecimal) x);
        } else {
            throw new SQLException(
                Constants.SQLExceptionMessages.SQL_TYPE_INFER + x.getClass().getCanonicalName());
        }
    }

    //Methods which are currently not supported

    public void addBatch() throws SQLException {
        throw new SQLFeatureNotSupportedException(
            Constants.SQLExceptionMessages.SQL_FEATURE_NOT_SUPPORTED);
    }

    public ParameterMetaData getParameterMetaData() throws SQLException {
        throw new SQLFeatureNotSupportedException(
            Constants.SQLExceptionMessages.SQL_FEATURE_NOT_SUPPORTED);
    }

    public void setNull(int parameterIndex, int sqlType, String typeName) throws SQLException {
        throw new SQLFeatureNotSupportedException(
            Constants.SQLExceptionMessages.SQL_FEATURE_NOT_SUPPORTED);
    }

    public void setAsciiStream(int parameterIndex, InputStream x, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException(
            Constants.SQLExceptionMessages.SQL_FEATURE_NOT_SUPPORTED);
    }

    public void setBinaryStream(int parameterIndex, InputStream x) throws SQLException {
        throw new SQLFeatureNotSupportedException(
            Constants.SQLExceptionMessages.SQL_FEATURE_NOT_SUPPORTED);
    }

    public void setCharacterStream(int parameterIndex, Reader reader, int length)
        throws SQLException {
        throw new SQLFeatureNotSupportedException(
            Constants.SQLExceptionMessages.SQL_FEATURE_NOT_SUPPORTED);
    }

    public void setObject(int parameterIndex, Object parameterObject, int targetSqlType,
        int scaleOrLength) throws SQLException {
        checkOpen();
        if (null == parameterObject) {
            setNull(parameterIndex, Types.OTHER);
        } else {
            try {
                switch (targetSqlType) {
                    case Types.BOOLEAN:
                        if (parameterObject instanceof Boolean) {
                            setBoolean(parameterIndex, ((Boolean) parameterObject).booleanValue());
                            break;
                        } else if (parameterObject instanceof String) {
                            setBoolean(parameterIndex,
                                "true".equalsIgnoreCase((String) parameterObject) || !"0"
                                    .equalsIgnoreCase((String) parameterObject));
                            break;
                        } else if (parameterObject instanceof Number) {
                            int intValue = ((Number) parameterObject).intValue();
                            setBoolean(parameterIndex, intValue != 0);
                            break;
                        } else {
                            throw new SQLException(
                                "Conversion from" + parameterObject.getClass().getName() +
                                    "to Types.Boolean is not Possible");
                        }
                    case Types.BIT:
                    case Types.TINYINT:
                    case Types.SMALLINT:
                    case Types.INTEGER:
                    case Types.BIGINT:
                    case Types.REAL:
                    case Types.FLOAT:
                    case Types.DOUBLE:
                    case Types.DECIMAL:
                    case Types.NUMERIC:
                        setNumericObject(parameterIndex, parameterObject, targetSqlType,
                            scaleOrLength);
                        break;
                    case Types.CHAR:
                    case Types.VARCHAR:
                    case Types.LONGVARCHAR:
                        if (parameterObject instanceof BigDecimal) {
                            setString(parameterIndex,
                                (StringUtils.fixDecimalExponent((parameterObject).toString())));
                        } else {
                            setString(parameterIndex, parameterObject.toString());
                        }
                        break;
                    case Types.CLOB:
                        if (parameterObject instanceof Clob) {
                            setClob(parameterIndex, (Clob) parameterObject);
                        } else {
                            setString(parameterIndex, parameterObject.toString());
                        }
                        break;
                    case Types.BINARY:
                    case Types.VARBINARY:
                    case Types.LONGVARBINARY:
                    case Types.BLOB:
                        if (parameterObject instanceof Blob) {
                            setBlob(parameterIndex, (Blob) parameterObject);
                        } else {
                            setBytes(parameterIndex, (byte[]) parameterObject);
                        }
                        break;
                    case Types.DATE:
                    case Types.TIMESTAMP:
                        java.util.Date parameterAsDate;
                        if (parameterObject instanceof String) {
                            ParsePosition pp = new ParsePosition(0);
                            DateFormat sdf = new SimpleDateFormat(
                                getDateTimePattern((String) parameterObject, false), Locale.US);
                            parameterAsDate = sdf.parse((String) parameterObject, pp);
                        } else {
                            parameterAsDate = (java.util.Date) parameterObject;
                        }
                        switch (targetSqlType) {
                            case Types.DATE:
                                if (parameterAsDate instanceof Date) {
                                    setDate(parameterIndex, (Date) parameterAsDate);
                                } else {
                                    setDate(parameterIndex, new Date(parameterAsDate.getTime()));
                                }
                                break;
                            case Types.TIMESTAMP:
                                if (parameterAsDate instanceof Timestamp) {
                                    setTimestamp(parameterIndex, (Timestamp) parameterAsDate);
                                } else {
                                    setTimestamp(parameterIndex,
                                        new Timestamp(parameterAsDate.getTime()));
                                }
                                break;
                        }
                        break;
                    case Types.TIME:
                        if (parameterObject instanceof String) {
                            DateFormat sdf = new SimpleDateFormat(
                                getDateTimePattern((String) parameterObject, true), Locale.US);
                            setTime(parameterIndex,
                                new Time(sdf.parse((String) parameterObject).getTime()));
                        } else if (parameterObject instanceof Timestamp) {
                            Timestamp timestamp = (Timestamp) parameterObject;
                            setTime(parameterIndex, new Time(timestamp.getTime()));
                        } else {
                            setTime(parameterIndex, (Time) parameterObject);
                        }
                        break;
                    default:
                        throw new SQLFeatureNotSupportedException(
                            Constants.SQLExceptionMessages.SQL_FEATURE_NOT_SUPPORTED);
                }
            } catch (Exception ex) {
                throw new SQLException(ex);
            }
        }
    }

    public void setAsciiStream(int parameterIndex, InputStream x, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException(
            Constants.SQLExceptionMessages.SQL_FEATURE_NOT_SUPPORTED);
    }

    public void setBinaryStream(int parameterIndex, InputStream x, long length)
        throws SQLException {
        throw new SQLFeatureNotSupportedException(
            Constants.SQLExceptionMessages.SQL_FEATURE_NOT_SUPPORTED);
    }

    public void setCharacterStream(int parameterIndex, Reader reader, long length)
        throws SQLException {
        throw new SQLFeatureNotSupportedException(
            Constants.SQLExceptionMessages.SQL_FEATURE_NOT_SUPPORTED);
    }

    public void setUnicodeStream(int parameterIndex, InputStream x, int length)
        throws SQLException {
        throw new SQLFeatureNotSupportedException(
            Constants.SQLExceptionMessages.SQL_FEATURE_NOT_SUPPORTED);
    }

    public void setRef(int parameterIndex, Ref x) throws SQLException {
        throw new SQLFeatureNotSupportedException(
            Constants.SQLExceptionMessages.SQL_FEATURE_NOT_SUPPORTED);
    }

    public void setBlob(int parameterIndex, Blob x) throws SQLException {
        throw new SQLFeatureNotSupportedException(
            Constants.SQLExceptionMessages.SQL_FEATURE_NOT_SUPPORTED);
    }

    public void setClob(int parameterIndex, Clob x) throws SQLException {
        throw new SQLFeatureNotSupportedException(
            Constants.SQLExceptionMessages.SQL_FEATURE_NOT_SUPPORTED);
    }

    public void setArray(int parameterIndex, Array x) throws SQLException {
        throw new SQLFeatureNotSupportedException(
            Constants.SQLExceptionMessages.SQL_FEATURE_NOT_SUPPORTED);
    }

    public ResultSetMetaData getMetaData() throws SQLException {
        throw new SQLFeatureNotSupportedException(
            Constants.SQLExceptionMessages.SQL_FEATURE_NOT_SUPPORTED);
    }

    public void setURL(int parameterIndex, URL x) throws SQLException {
        throw new SQLFeatureNotSupportedException(
            Constants.SQLExceptionMessages.SQL_FEATURE_NOT_SUPPORTED);
    }

    public void setRowId(int parameterIndex, RowId x) throws SQLException {
        throw new SQLFeatureNotSupportedException(
            Constants.SQLExceptionMessages.SQL_FEATURE_NOT_SUPPORTED);
    }

    public void setNString(int parameterIndex, String value) throws SQLException {
        throw new SQLFeatureNotSupportedException(
            Constants.SQLExceptionMessages.SQL_FEATURE_NOT_SUPPORTED);
    }

    public void setNCharacterStream(int parameterIndex, Reader value, long length)
        throws SQLException {
        throw new SQLFeatureNotSupportedException(
            Constants.SQLExceptionMessages.SQL_FEATURE_NOT_SUPPORTED);
    }

    public void setNClob(int parameterIndex, NClob value) throws SQLException {
        throw new SQLFeatureNotSupportedException(
            Constants.SQLExceptionMessages.SQL_FEATURE_NOT_SUPPORTED);
    }

    public void setClob(int parameterIndex, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException(
            Constants.SQLExceptionMessages.SQL_FEATURE_NOT_SUPPORTED);
    }

    public void setBlob(int parameterIndex, InputStream inputStream, long length)
        throws SQLException {
        throw new SQLFeatureNotSupportedException(
            Constants.SQLExceptionMessages.SQL_FEATURE_NOT_SUPPORTED);
    }

    public void setNClob(int parameterIndex, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException(
            Constants.SQLExceptionMessages.SQL_FEATURE_NOT_SUPPORTED);
    }

    public void setSQLXML(int parameterIndex, SQLXML xmlObject) throws SQLException {
        throw new SQLFeatureNotSupportedException(
            Constants.SQLExceptionMessages.SQL_FEATURE_NOT_SUPPORTED);
    }

    public void setAsciiStream(int parameterIndex, InputStream x) throws SQLException {
        throw new SQLFeatureNotSupportedException(
            Constants.SQLExceptionMessages.SQL_FEATURE_NOT_SUPPORTED);
    }

    public void setBinaryStream(int parameterIndex, InputStream x, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException(
            Constants.SQLExceptionMessages.SQL_FEATURE_NOT_SUPPORTED);
    }

    public void setCharacterStream(int parameterIndex, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException(
            Constants.SQLExceptionMessages.SQL_FEATURE_NOT_SUPPORTED);
    }

    public void setNCharacterStream(int parameterIndex, Reader value) throws SQLException {
        throw new SQLFeatureNotSupportedException(
            Constants.SQLExceptionMessages.SQL_FEATURE_NOT_SUPPORTED);
    }

    public void setClob(int parameterIndex, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException(
            Constants.SQLExceptionMessages.SQL_FEATURE_NOT_SUPPORTED);
    }

    public void setBlob(int parameterIndex, InputStream inputStream) throws SQLException {
        throw new SQLFeatureNotSupportedException(
            Constants.SQLExceptionMessages.SQL_FEATURE_NOT_SUPPORTED);
    }

    public void setNClob(int parameterIndex, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException(
            Constants.SQLExceptionMessages.SQL_FEATURE_NOT_SUPPORTED);
    }

    public void setObject(int parameterIndex, Object parameterObject, int targetSqlType)
        throws SQLException {
        if (!(parameterObject instanceof BigDecimal)) {
            setObject(parameterIndex, parameterObject, targetSqlType, 0);
        } else {
            setObject(parameterIndex, parameterObject, targetSqlType,
                ((BigDecimal) parameterObject).scale());
        }
    }

    private void setNumericObject(int parameterIndex, Object parameterObj, int targetSqlType,
        int scale) throws SQLException {
        Number parameterAsNum;
        if (parameterObj instanceof Boolean) {
            parameterAsNum =
                ((Boolean) parameterObj).booleanValue() ? Integer.valueOf(1) : Integer.valueOf(0);
        } else if (parameterObj instanceof String) {
            switch (targetSqlType) {
                case Types.BIT:
                    if ("1".equals(parameterObj) || "0".equals(parameterObj)) {
                        parameterAsNum = Integer.valueOf((String) parameterObj);
                    } else {
                        boolean parameterAsBoolean = "true".equalsIgnoreCase((String) parameterObj);

                        parameterAsNum =
                            parameterAsBoolean ? Integer.valueOf(1) : Integer.valueOf(0);
                    }
                    break;

                case Types.TINYINT:
                case Types.SMALLINT:
                case Types.INTEGER:
                    parameterAsNum = Integer.valueOf((String) parameterObj);
                    break;

                case Types.BIGINT:
                    parameterAsNum = Long.valueOf((String) parameterObj);
                    break;

                case Types.REAL:
                    parameterAsNum = Float.valueOf((String) parameterObj);
                    break;

                case Types.FLOAT:
                case Types.DOUBLE:
                    parameterAsNum = Double.valueOf((String) parameterObj);
                    break;

                case Types.DECIMAL:
                case Types.NUMERIC:
                default:
                    parameterAsNum = new java.math.BigDecimal((String) parameterObj);
            }
        } else {
            parameterAsNum = (Number) parameterObj;
        }
        switch (targetSqlType) {
            case Types.BIT:
            case Types.TINYINT:
            case Types.SMALLINT:
            case Types.INTEGER:
                setInt(parameterIndex, parameterAsNum.intValue());
                break;

            case Types.BIGINT:
                setLong(parameterIndex, parameterAsNum.longValue());
                break;

            case Types.REAL:
                setFloat(parameterIndex, parameterAsNum.floatValue());
                break;

            case Types.FLOAT:
            case Types.DOUBLE:
                setDouble(parameterIndex, parameterAsNum.doubleValue());
                break;

            case Types.DECIMAL:
            case Types.NUMERIC:

                if (parameterAsNum instanceof java.math.BigDecimal) {
                    BigDecimal scaledBigDecimal = null;
                    try {
                        scaledBigDecimal = ((java.math.BigDecimal) parameterAsNum).setScale(scale);
                    } catch (ArithmeticException ex) {
                        try {
                            scaledBigDecimal = ((java.math.BigDecimal) parameterAsNum)
                                .setScale(scale, BigDecimal.ROUND_HALF_UP);
                        } catch (ArithmeticException arEx) {
                            throw new SQLException("Can't set the scale of '" + scale +
                                "' for Decimal Argument" + parameterAsNum);
                        }
                    }
                    setBigDecimal(parameterIndex, scaledBigDecimal);
                } else if (parameterAsNum instanceof java.math.BigInteger) {
                    setBigDecimal(parameterIndex,
                        new java.math.BigDecimal((java.math.BigInteger) parameterAsNum, scale));
                } else {
                    setBigDecimal(parameterIndex,
                        new java.math.BigDecimal(parameterAsNum.doubleValue()));
                }
                break;
        }
    }

    /*
     * DateTime Format Parsing Logic from Mysql JDBC
     */
    private final String getDateTimePattern(String dt, boolean toTime) throws Exception {
        int dtLength = (dt != null) ? dt.length() : 0;

        if ((dtLength >= 8) && (dtLength <= 10)) {
            int dashCount = 0;
            boolean isDateOnly = true;

            for (int i = 0; i < dtLength; i++) {
                char c = dt.charAt(i);

                if (!Character.isDigit(c) && (c != '-')) {
                    isDateOnly = false;

                    break;
                }

                if (c == '-') {
                    dashCount++;
                }
            }

            if (isDateOnly && (dashCount == 2)) {
                return "yyyy-MM-dd";
            }
        }

        // Special case - time-only
        boolean colonsOnly = true;
        for (int i = 0; i < dtLength; i++) {
            char c = dt.charAt(i);

            if (!Character.isDigit(c) && (c != ':')) {
                colonsOnly = false;

                break;
            }
        }

        if (colonsOnly) {
            return "HH:mm:ss";
        }

        int n;
        int z;
        int count;
        int maxvecs;
        char c;
        char separator;
        StringReader reader = new StringReader(dt + " ");
        ArrayList<Object[]> vec = new ArrayList<>();
        ArrayList<Object[]> vecRemovelist = new ArrayList<>();
        Object[] nv = new Object[3];
        Object[] v;
        nv[0] = Character.valueOf('y');
        nv[1] = new StringBuilder();
        nv[2] = Integer.valueOf(0);
        vec.add(nv);

        if (toTime) {
            nv = new Object[3];
            nv[0] = Character.valueOf('h');
            nv[1] = new StringBuilder();
            nv[2] = Integer.valueOf(0);
            vec.add(nv);
        }

        while ((z = reader.read()) != -1) {
            separator = (char) z;
            maxvecs = vec.size();

            for (count = 0; count < maxvecs; count++) {
                v = vec.get(count);
                n = ((Integer) v[2]).intValue();
                c = getSuccessor(((Character) v[0]).charValue(), n);

                if (!Character.isLetterOrDigit(separator)) {
                    if ((c == ((Character) v[0]).charValue()) && (c != 'S')) {
                        vecRemovelist.add(v);
                    } else {
                        ((StringBuilder) v[1]).append(separator);

                        if ((c == 'X') || (c == 'Y')) {
                            v[2] = Integer.valueOf(4);
                        }
                    }
                } else {
                    if (c == 'X') {
                        c = 'y';
                        nv = new Object[3];
                        nv[1] = (new StringBuilder((v[1]).toString())).append('M');
                        nv[0] = Character.valueOf('M');
                        nv[2] = Integer.valueOf(1);
                        vec.add(nv);
                    } else if (c == 'Y') {
                        c = 'M';
                        nv = new Object[3];
                        nv[1] = (new StringBuilder((v[1]).toString())).append('d');
                        nv[0] = Character.valueOf('d');
                        nv[2] = Integer.valueOf(1);
                        vec.add(nv);
                    }

                    ((StringBuilder) v[1]).append(c);
                    if (c == ((Character) v[0]).charValue()) {
                        v[2] = Integer.valueOf(n + 1);
                    } else {
                        v[0] = Character.valueOf(c);
                        v[2] = Integer.valueOf(1);
                    }
                }
            }

            int size = vecRemovelist.size();
            for (int i = 0; i < size; i++) {
                v = vecRemovelist.get(i);
                vec.remove(v);
            }
            vecRemovelist.clear();
        }

        int size = vec.size();
        for (int i = 0; i < size; i++) {
            v = vec.get(i);
            c = ((Character) v[0]).charValue();
            n = ((Integer) v[2]).intValue();

            boolean bk = getSuccessor(c, n) != c;
            boolean atEnd = (((c == 's') || (c == 'm') || ((c == 'h') && toTime)) && bk);
            boolean finishesAtDate = (bk && (c == 'd') && !toTime);
            boolean containsEnd = ((v[1]).toString().indexOf('W') != -1);

            if ((!atEnd && !finishesAtDate) || (containsEnd)) {
                vecRemovelist.add(v);
            }
        }

        size = vecRemovelist.size();

        for (int i = 0; i < size; i++) {
            vec.remove(vecRemovelist.get(i));
        }

        vecRemovelist.clear();
        v = vec.get(0); // might throw exception

        StringBuilder format = (StringBuilder) v[1];
        format.setLength(format.length() - 1);

        return format.toString();
    }

    private final char getSuccessor(char c, int n) {
        return ((c == 'y') && (n == 2)) ?
            'X' :
            (((c == 'y') && (n < 4)) ?
                'y' :
                ((c == 'y') ?
                    'M' :
                    (((c == 'M') && (n == 2)) ?
                        'Y' :
                        (((c == 'M') && (n < 3)) ?
                            'M' :
                            ((c == 'M') ?
                                'd' :
                                (((c == 'd') && (n < 2)) ?
                                    'd' :
                                    ((c == 'd') ?
                                        'H' :
                                        (((c == 'H') && (n < 2)) ?
                                            'H' :
                                            ((c == 'H') ?
                                                'm' :
                                                (((c == 'm') && (n < 2)) ?
                                                    'm' :
                                                    ((c == 'm') ?
                                                        's' :
                                                        (((c == 's') && (n < 2)) ?
                                                            's' :
                                                            'W'))))))))))));
    }
}
