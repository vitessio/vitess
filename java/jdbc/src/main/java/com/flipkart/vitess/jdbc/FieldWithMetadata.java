package com.flipkart.vitess.jdbc;

import com.flipkart.vitess.util.Constants;
import com.flipkart.vitess.util.MysqlDefs;
import com.youtube.vitess.proto.Query;

import java.sql.SQLException;
import java.sql.Types;

public class FieldWithMetadata {

    private final VitessConnection connection;
    private final Query.Field field;
    private final Query.Type vitessType;
    private final boolean isSingleBit;
    private final int precisionAdjustFactor;

    private int javaType;
    private int colFlag;
    private String encoding;
    private String collationName;
    private int collationIndex;
    private int maxBytesPerChar;

    public FieldWithMetadata(VitessConnection connection, Query.Field field) throws SQLException {
        this.connection = connection;
        this.field = field;
        this.colFlag = field.getFlags();
        this.vitessType = field.getType();
        this.collationIndex = field.getCharset();

        // Map MySqlTypes to java.sql Types
        if (MysqlDefs.vitesstoMySqlType.containsKey(vitessType)) {
            this.javaType = MysqlDefs.vitesstoMySqlType.get(vitessType);
        } else if (field.getType().equals(Query.Type.TUPLE)) {
            throw new SQLException(Constants.SQLExceptionMessages.INVALID_COLUMN_TYPE);
        } else {
            throw new SQLException(Constants.SQLExceptionMessages.UNKNOWN_COLUMN_TYPE);
        }

        // All of the below remapping and metadata fields require the extra
        // fields included when includeFields=IncludedFields.ALL
        if (connection != null && connection.isIncludeAllFields()) {
            // Re-map TINYINT(1) as bit or pseudo-boolean
            if (this.javaType == Types.TINYINT && this.field.getColumnLength() == 1 && connection.getTinyInt1isBit()) {
                this.javaType = Types.BIT;
            }

            if (!isNativeNumericType() && !isNativeDateTimeType()) {
                if (this.javaType == Types.BIT) {
                    this.isSingleBit = field.getColumnLength() == 0 || field.getColumnLength() == 1;
                } else {
                    this.isSingleBit = false;
                }
            } else {
                this.isSingleBit = false;
            }

            // Precision can be calculated from column length, but needs
            // to be adjusted for the extra bytes used by the negative sign
            // and decimal points, where appropriate.
            if (isSigned()) {
                switch (javaType) {
                    case Types.FLOAT:
                    case Types.REAL:
                    case Types.DOUBLE:
                    case Types.BIT:
                        // float/real/double are the same regardless of sign/decimal
                        // bit values can't actually be signed
                        this.precisionAdjustFactor = 0;
                        break;
                    default:
                        // other types we adjust for the negative symbol, and decimal
                        // symbol if there are decimals
                        this.precisionAdjustFactor = getDecimals() > 0 ? -2 : -1;
                }
            } else {
                switch (javaType) {
                    case Types.DECIMAL:
                    case Types.NUMERIC:
                        // adjust for the decimal
                        this.precisionAdjustFactor = -1;
                        break;
                    default:
                        // all other types need no adjustment
                        this.precisionAdjustFactor = 0;
                }
            }
        } else {
            // Defaults to appease final variables when not including all fields
            this.isSingleBit = false;
            this.precisionAdjustFactor = 0;
        }
    }

    private boolean isNativeNumericType() {
        switch (this.javaType) {
            case Types.TINYINT:
            case Types.SMALLINT:
            case Types.INTEGER:
            case Types.BIGINT:
            case Types.FLOAT:
            case Types.DOUBLE:
            case Types.REAL:
            case Types.DECIMAL:
                return true;
            default:
                return false;
        }
    }

    private boolean isNativeDateTimeType() {
        switch (this.javaType) {
            case Types.DATE:
            case Types.TIME:
            case Types.TIMESTAMP:
                return true;
            default:
                return false;
        }
    }

    public VitessConnection getConnection() throws SQLException {
        checkConnection();
        return connection;
    }

    private void checkConnection() throws SQLException {
        if (connection == null) {
            throw new SQLException(Constants.SQLExceptionMessages.SQL_FEATURE_NOT_SUPPORTED);
        }
    }

    public boolean isAutoIncrement() throws SQLException {
        checkConnection();
        if (!connection.isIncludeAllFields()) {
            return false;
        }
        return ((this.colFlag & Query.MySqlFlag.AUTO_INCREMENT_FLAG_VALUE) > 0);
    }

    public boolean isBinary() throws SQLException {
        checkConnection();
        if (!connection.isIncludeAllFields()) {
            return false;
        }
        return ((this.colFlag & Query.MySqlFlag.BINARY_FLAG_VALUE) > 0);
    }

    public boolean isBlob() throws SQLException {
        checkConnection();
        if (!connection.isIncludeAllFields()) {
            return false;
        }
        return ((this.colFlag & Query.MySqlFlag.BLOB_FLAG_VALUE) > 0);
    }

    public boolean isMultipleKey() throws SQLException {
        checkConnection();
        if (!connection.isIncludeAllFields()) {
            return false;
        }
        return ((this.colFlag & Query.MySqlFlag.MULTIPLE_KEY_FLAG_VALUE) > 0);
    }

    boolean isNotNull() throws SQLException {
        checkConnection();
        if (!connection.isIncludeAllFields()) {
            return true;
        }
        return ((this.colFlag & Query.MySqlFlag.NOT_NULL_FLAG_VALUE) > 0);
    }

    public boolean isZeroFill() throws SQLException {
        checkConnection();
        if (!connection.isIncludeAllFields()) {
            return false;
        }
        return ((this.colFlag & Query.MySqlFlag.ZEROFILL_FLAG_VALUE) > 0);
    }

    public boolean isPrimaryKey() throws SQLException {
        checkConnection();
        if (!connection.isIncludeAllFields()) {
            return false;
        }
        return ((this.colFlag & Query.MySqlFlag.PRI_KEY_FLAG_VALUE) > 0);
    }

    public boolean isUniqueKey() throws SQLException {
        checkConnection();
        if (!connection.isIncludeAllFields()) {
            return false;
        }
        return ((this.colFlag & Query.MySqlFlag.UNIQUE_KEY_FLAG_VALUE) > 0);
    }

    public boolean isUnsigned() throws SQLException {
        checkConnection();
        if (!connection.isIncludeAllFields()) {
            return true;
        }
        return ((this.colFlag & Query.MySqlFlag.UNSIGNED_FLAG_VALUE) > 0);
    }

    public boolean isSigned() throws SQLException {
        checkConnection();
        return !isUnsigned();
    }

    boolean isOpaqueBinary() throws SQLException {
        checkConnection();
        return false;
    }

    /**
     * Is this field _definitely_ not writable?
     *
     * @return true if this field can not be written to in an INSERT/UPDATE
     * statement.
     */
    boolean isReadOnly() throws SQLException {
        checkConnection();
        if (!connection.isIncludeAllFields()) {
            return false;
        }
        String orgColumnName = getOrgName();
        String orgTableName = getOrgTable();
        return !(orgColumnName != null && orgColumnName.length() > 0 && orgTableName != null && orgTableName.length() > 0);
    }

    public String getName() {
        return field.getName();
    }

    public String getTable() throws SQLException {
        checkConnection();
        if (!connection.isIncludeAllFields()) {
            return null;
        }
        return field.getTable();
    }

    public String getOrgTable() throws SQLException {
        checkConnection();
        if (!connection.isIncludeAllFields()) {
            return null;
        }
        return field.getOrgTable();
    }

    public String getDatabase() throws SQLException {
        checkConnection();
        if (!connection.isIncludeAllFields()) {
            return null;
        }
        return field.getDatabase();
    }

    public String getOrgName() throws SQLException {
        checkConnection();
        if (!connection.isIncludeAllFields()) {
            return null;
        }
        return field.getOrgName();
    }

    public int getColumnLength() throws SQLException {
        checkConnection();
        if (!connection.isIncludeAllFields()) {
            return 0;
        }
        return field.getColumnLength();
    }

    public int getDecimals() throws SQLException {
        checkConnection();
        if (!connection.isIncludeAllFields()) {
            return 0;
        }
        return field.getDecimals();
    }

    public int getJavaType() {
        return javaType;
    }

    public Query.Type getVitessType() {
        return vitessType;
    }

    public int getVitessTypeValue() {
        return field.getTypeValue();
    }

    /**
     * Precision can be calculated from column length, but needs
     * to be adjusted for the extra values that can be included for the various
     * numeric types
     */
    public int getPrecisionAdjustFactor() throws SQLException {
        checkConnection();
        if (!connection.isIncludeAllFields()) {
            return 0;
        }
        return precisionAdjustFactor;
    }

    public boolean isSingleBit() throws SQLException {
        checkConnection();
        if (!connection.isIncludeAllFields()) {
            return false;
        }
        return isSingleBit;
    }

    @Override
    public String toString() {
        try {
            StringBuilder asString = new StringBuilder();
            asString.append(getClass().getCanonicalName());
            asString.append("[");
            asString.append("catalog=");
            asString.append(this.getDatabase());
            asString.append(",tableName=");
            asString.append(this.getTable());
            asString.append(",originalTableName=");
            asString.append(this.getOrgTable());
            asString.append(",columnName=");
            asString.append(this.getName());
            asString.append(",originalColumnName=");
            asString.append(this.getOrgName());
            asString.append(",vitessType=");
            asString.append(getVitessType());
            asString.append("(");
            asString.append(getJavaType());
            asString.append(")");
            asString.append(",flags=");
            if (isAutoIncrement()) {
                asString.append("AUTO_INCREMENT");
            }
            if (isPrimaryKey()) {
                asString.append(" PRIMARY_KEY");
            }
            if (isUniqueKey()) {
                asString.append(" UNIQUE_KEY");
            }
            if (isBinary()) {
                asString.append(" BINARY");
            }
            if (isBlob()) {
                asString.append(" BLOB");
            }
            if (isMultipleKey()) {
                asString.append(" MULTI_KEY");
            }
            if (isUnsigned()) {
                asString.append(" UNSIGNED");
            }
            if (isZeroFill()) {
                asString.append(" ZEROFILL");
            }
            return asString.toString();
        } catch (Throwable t) {
            return super.toString();
        }
    }
}
