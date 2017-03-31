package io.vitess.jdbc;

import com.google.common.collect.ImmutableList;
import io.vitess.proto.Query;
import io.vitess.util.Constants;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.List;
import java.util.logging.Logger;


/**
 * Created by harshit.gangal on 25/01/16.
 */
public class VitessResultSetMetaData implements ResultSetMetaData {

    /* Get actual class name to be printed on */
    private static Logger logger = Logger.getLogger(VitessResultSetMetaData.class.getName());
    private List<FieldWithMetadata> fields;

    public VitessResultSetMetaData(List<FieldWithMetadata> fields) throws SQLException {
        this.fields = ImmutableList.copyOf(fields);
    }

    public int getColumnCount() throws SQLException {
        return fields.size();
    }

    public boolean isAutoIncrement(int column) throws SQLException {
        return getField(column).isAutoIncrement();
    }

    public boolean isCaseSensitive(int column) throws SQLException {
        FieldWithMetadata field = getField(column);
        switch (field.getJavaType()) {
            case Types.BIT:
            case Types.TINYINT:
            case Types.SMALLINT:
            case Types.INTEGER:
            case Types.BIGINT:
            case Types.FLOAT:
            case Types.REAL:
            case Types.DOUBLE:
            case Types.DATE:
            case Types.DECIMAL:
            case Types.NUMERIC:
            case Types.TIME:
            case Types.TIMESTAMP:
                return false;
            case Types.CHAR:
            case Types.VARCHAR:
            case Types.LONGVARCHAR:
                if (field.isBinary() || !field.getConnectionProperties().isIncludeAllFields()) {
                    return true;
                }
                try {
                    String collationName = field.getCollation();
                    return collationName != null && !collationName.endsWith("_ci");
                } catch (SQLException e) {
                    if (e.getCause() instanceof ArrayIndexOutOfBoundsException) {
                        return false;
                    } else {
                        throw e;
                    }
                }
            default:
                return true;
        }
    }

    public boolean isSearchable(int column) throws SQLException {
        return true;
    }

    public boolean isCurrency(int column) throws SQLException {
        return false;
    }

    /**
     * Indicates the nullability of values in the designated column.
     *
     * @param column the first column is 1, the second is 2, ...
     * @return the nullability status of the given column; one of <code>columnNoNulls</code>,
     *          <code>columnNullable</code> or <code>columnNullableUnknown</code>
     * @exception SQLException if a database access error occurs
     */
    public int isNullable(int column) throws SQLException {
        FieldWithMetadata field = getField(column);
        if (!field.getConnectionProperties().isIncludeAllFields()) {
            return ResultSetMetaData.columnNullableUnknown;
        }
        return field.isNotNull() ? ResultSetMetaData.columnNoNulls : ResultSetMetaData.columnNullable;
    }

    public boolean isSigned(int column) throws SQLException {
        return getField(column).isSigned();
    }

    public int getColumnDisplaySize(int column) throws SQLException {
        FieldWithMetadata field = getField(column);
        if (!field.getConnectionProperties().isIncludeAllFields()) {
            return 0;
        }
        // If we can't find a charset, we'll return 0. In that case assume 1 byte per char
        return field.getColumnLength() / Math.max(field.getMaxBytesPerCharacter(), 1);
    }

    public String getColumnLabel(int column) throws SQLException {
        return getField(column).getName();
    }

    public String getColumnName(int column) throws SQLException {
        return getField(column).getName();
    }

    public String getSchemaName(int column) throws SQLException {
        return getField(column).getDatabase();
    }

    public int getPrecision(int column) throws SQLException {
        FieldWithMetadata field = getField(column);
        if (!field.getConnectionProperties().isIncludeAllFields()) {
            return 0;
        }
        if (isDecimalType(field.getJavaType(), field.getVitessTypeValue())) {
            return field.getColumnLength() + field.getPrecisionAdjustFactor();
        }
        switch (field.getJavaType()) {
            case Types.VARBINARY:
            case Types.LONGVARBINARY:
                return field.getColumnLength();
            default:
                return field.getColumnLength() / field.getMaxBytesPerCharacter();
        }
    }

    private static boolean isDecimalType(int javaType, int vitessType) {
        switch (javaType) {
            case Types.BIT:
            case Types.TINYINT:
            case Types.INTEGER:
            case Types.BIGINT:
            case Types.FLOAT:
            case Types.REAL:
            case Types.DOUBLE:
            case Types.NUMERIC:
            case Types.DECIMAL:
                return true;
            case Types.SMALLINT:
                return vitessType != Query.Type.YEAR_VALUE;
            default:
                return false;
        }
    }

    public int getScale(int column) throws SQLException {
        FieldWithMetadata field = getField(column);
        if (isDecimalType(field.getJavaType(), field.getVitessTypeValue())) {
            return getField(column).getDecimals();
        }
        return 0;
    }

    public String getTableName(int column) throws SQLException {
        return getField(column).getTable();
    }

    public String getCatalogName(int column) throws SQLException {
        return getField(column).getDatabase();
    }

    public int getColumnType(int column) throws SQLException {
        return getField(column).getJavaType();
    }

    public String getColumnTypeName(int column) throws SQLException {
        FieldWithMetadata field = getField(column);

        int vitessTypeValue = field.getVitessTypeValue();
        int javaType = field.getJavaType();

        switch (vitessTypeValue) {
            case Query.Type.BIT_VALUE:
                return "BIT";

            case Query.Type.DECIMAL_VALUE:
                return "DECIMAL";

            case Query.Type.INT8_VALUE:
                return "TINYINT";
            case Query.Type.UINT8_VALUE:
                return "TINYINT UNSIGNED";

            case Query.Type.INT16_VALUE:
                return "SMALLINT";
            case Query.Type.UINT16_VALUE:
                return "SMALLINT UNSIGNED";

            case Query.Type.INT24_VALUE:
                return "MEDIUMINT";
            case Query.Type.UINT24_VALUE:
                return "MEDIUMINT UNSIGNED";

            case Query.Type.INT32_VALUE:
                return "INT";
            case Query.Type.UINT32_VALUE:
                return "INT UNSIGNED";

            case Query.Type.INT64_VALUE:
                return "BIGINT";
            case Query.Type.UINT64_VALUE:
                return "BIGINT UNSIGNED";

            case Query.Type.FLOAT32_VALUE:
                return "FLOAT";

            case Query.Type.FLOAT64_VALUE:
                return "DOUBLE";

            case Query.Type.NULL_TYPE_VALUE:
                return "NULL";

            case Query.Type.TIMESTAMP_VALUE:
                return "TIMESTAMP";

            case Query.Type.DATE_VALUE:
                return "DATE";

            case Query.Type.TIME_VALUE:
                return "TIME";

            case Query.Type.DATETIME_VALUE:
                return "DATETIME";

            case Query.Type.BLOB_VALUE:
                return "BLOB";

            case Query.Type.TEXT_VALUE:
                return "TEXT";

            case Query.Type.VARCHAR_VALUE:
                return "VARCHAR";

            case Query.Type.VARBINARY_VALUE:
                if (javaType == Types.VARBINARY) {
                    return "VARBINARY";
                }
                return "VARCHAR";

            case Query.Type.BINARY_VALUE:
                if (javaType == Types.BINARY) {
                    return "BINARY";
                }
                return "CHAR";

            case Query.Type.CHAR_VALUE:
                return "CHAR";

            case Query.Type.ENUM_VALUE:
                return "ENUM";

            case Query.Type.YEAR_VALUE:
                return "YEAR";

            case Query.Type.SET_VALUE:
                return "SET";

            case Query.Type.TUPLE_VALUE:
                return "TUPLE";

            default:
                return "UNKNOWN";
        }
    }

    public boolean isReadOnly(int column) throws SQLException {
        return getField(column).isReadOnly();
    }

    public boolean isWritable(int column) throws SQLException {
        return !isReadOnly(column);
    }

    public boolean isDefinitelyWritable(int column) throws SQLException {
        return isWritable(column);
    }

    public String getColumnClassName(int column) throws SQLException {
        FieldWithMetadata field = getField(column);
        if (!field.getConnectionProperties().isIncludeAllFields()) {
            return null;
        }
        return getClassNameForJavaType(field.getJavaType(), field.getVitessTypeValue(), field.isUnsigned(),
            field.isBinary() || field.isBlob(), field.isOpaqueBinary(), field.getConnectionProperties().getYearIsDateType());
    }

    public <T> T unwrap(Class<T> iface) throws SQLException {
        return null;
    }

    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return false;
    }

    private FieldWithMetadata getField(int columnIndex) throws SQLException {
        if (columnIndex >= 1 && columnIndex <= this.fields.size()) {
            return fields.get(columnIndex - 1);
        } else {
            throw new SQLException(
                Constants.SQLExceptionMessages.INVALID_COLUMN_INDEX + ": " + columnIndex);
        }
    }

    private String getClassNameForJavaType(int javaType, int vitessType, boolean isUnsigned, boolean isBinaryOrBlob, boolean isOpaqueBinary,
                                          boolean treatYearAsDate) {
        switch (javaType) {
            case Types.BIT:
            case Types.BOOLEAN:
                return "java.lang.Boolean";
            case Types.TINYINT:
                if (isUnsigned) {
                    return "java.lang.Integer";
                }
                return "java.lang.Integer";
            case Types.SMALLINT:
                if (vitessType == Query.Type.YEAR_VALUE) {
                    return treatYearAsDate ? "java.sql.Date" : "java.lang.Short";
                }
                if (isUnsigned) {
                    return "java.lang.Integer";
                }
                return "java.lang.Integer";
            case Types.INTEGER:
                if (!isUnsigned || vitessType == Query.Type.UINT24_VALUE) {
                    return "java.lang.Integer";
                }
                return "java.lang.Long";
            case Types.BIGINT:
                if (!isUnsigned) {
                    return "java.lang.Long";
                }
                return "java.math.BigInteger";
            case Types.DECIMAL:
            case Types.NUMERIC:
                return "java.math.BigDecimal";
            case Types.REAL:
                return "java.lang.Float";
            case Types.FLOAT:
            case Types.DOUBLE:
                return "java.lang.Double";
            case Types.CHAR:
            case Types.VARCHAR:
            case Types.LONGVARCHAR:
                if (!isOpaqueBinary) {
                    return "java.lang.String";
                }
                return "[B";
            case Types.BINARY:
            case Types.VARBINARY:
            case Types.LONGVARBINARY:
                if (isBinaryOrBlob) {
                    return "[B";
                } else {
                    return "java.lang.String";
                }
            case Types.DATE:
                return treatYearAsDate ? "java.sql.Date" : "java.lang.Short";
            case Types.TIME:
                return "java.sql.Time";
            case Types.TIMESTAMP:
                return "java.sql.Timestamp";
            default:
                return "java.lang.Object";
        }
    }
}
