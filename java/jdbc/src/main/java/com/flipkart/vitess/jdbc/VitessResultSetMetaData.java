package com.flipkart.vitess.jdbc;

import com.flipkart.vitess.util.Constants;
import com.flipkart.vitess.util.MysqlDefs;
import com.google.common.collect.ImmutableList;
import com.youtube.vitess.proto.Query;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.List;
import java.util.logging.Logger;


/**
 * Created by harshit.gangal on 25/01/16.
 */
public class VitessResultSetMetaData implements ResultSetMetaData {

    /* Get actual class name to be printed on */
    private static Logger logger = Logger.getLogger(VitessResultSetMetaData.class.getName());
    private List<Query.Field> fields;

    public VitessResultSetMetaData(List<Query.Field> fields) {
        this.fields = ImmutableList.copyOf(fields);
    }

    public int getColumnCount() throws SQLException {
        return fields.size();
    }

    public boolean isAutoIncrement(int column) throws SQLException {
        return false;
    }

    public boolean isCaseSensitive(int column) throws SQLException {
        return true;
    }

    public boolean isSearchable(int column) throws SQLException {
        return false;
    }

    public boolean isCurrency(int column) throws SQLException {
        return false;
    }

    public int isNullable(int column) throws SQLException {
        return 0;
    }

    public boolean isSigned(int column) throws SQLException {
        return false;
    }

    public int getColumnDisplaySize(int column) throws SQLException {
        return 0;
    }

    public String getColumnLabel(int column) throws SQLException {
        Query.Field field = getField(column);
        return field.getName();
    }

    public String getColumnName(int column) throws SQLException {
        Query.Field field = getField(column);
        return field.getName();
    }

    public String getSchemaName(int column) throws SQLException {
        return null;
    }

    public int getPrecision(int column) throws SQLException {
        return 0;
    }

    public int getScale(int column) throws SQLException {
        return 0;
    }

    public String getTableName(int column) throws SQLException {
        return null;
    }

    public String getCatalogName(int column) throws SQLException {
        return null;
    }

    public int getColumnType(int column) throws SQLException {
        Query.Field field = getField(column);
        if (MysqlDefs.vitesstoMySqlType.containsKey(field.getType())) {
            return MysqlDefs.vitesstoMySqlType.get(field.getType());
        } else if (field.getType().equals(Query.Type.TUPLE)) {
            throw new SQLException(Constants.SQLExceptionMessages.INVALID_COLUMN_TYPE);
        } else {
            throw new SQLException(Constants.SQLExceptionMessages.UNKNOWN_COLUMN_TYPE);
        }
    }

    public String getColumnTypeName(int column) throws SQLException {
        Query.Field field = getField(column);
        return field.getType().toString();
    }

    public boolean isReadOnly(int column) throws SQLException {
        return false;
    }

    public boolean isWritable(int column) throws SQLException {
        return !isReadOnly(column);
    }

    public boolean isDefinitelyWritable(int column) throws SQLException {
        return !isReadOnly(column);
    }

    public String getColumnClassName(int column) throws SQLException {
        return null;
    }

    public <T> T unwrap(Class<T> iface) throws SQLException {
        return null;
    }

    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return false;
    }

    protected Query.Field getField(int columnIndex) throws SQLException {
        if (columnIndex >= 1 && columnIndex <= this.fields.size()) {
            return fields.get(columnIndex - 1);
        } else {
            throw new SQLException(
                Constants.SQLExceptionMessages.INVALID_COLUMN_INDEX + ": " + columnIndex);
        }
    }
}
