package io.vitess.jdbc;

import java.sql.ParameterMetaData;
import java.sql.SQLException;
import java.sql.Types;

public class VitessParameterMetaData implements ParameterMetaData {

    private final int parameterCount;

    /**
     * This implementation (and defaults below) is equivalent to
     * mysql-connector-java's "simple" (non-server)
     * statement metadata
     */
    VitessParameterMetaData(int count) {
        this.parameterCount = count;
    }

    @Override
    public int getParameterCount() throws SQLException {
        return parameterCount;
    }

    @Override
    public int isNullable(int param) throws SQLException {
        throw new SQLException("Parameter metadata not available for the given statement");
    }

    @Override
    public boolean isSigned(int param) throws SQLException {
        checkBounds(param);
        return false;
    }

    @Override
    public int getPrecision(int param) throws SQLException {
        checkBounds(param);
        return 0;
    }

    @Override
    public int getScale(int param) throws SQLException {
        checkBounds(param);
        return 0;
    }

    @Override
    public int getParameterType(int param) throws SQLException {
        checkBounds(param);
        return Types.VARCHAR;
    }

    @Override
    public String getParameterTypeName(int param) throws SQLException {
        checkBounds(param);
        return "VARCHAR";
    }

    @Override
    public String getParameterClassName(int param) throws SQLException {
        checkBounds(param);
        return "java.lang.String";
    }

    @Override
    public int getParameterMode(int param) throws SQLException {
        return ParameterMetaData.parameterModeIn;
    }

    private void checkBounds(int paramNumber) throws SQLException {
        if (paramNumber < 1) {
            throw new SQLException("Parameter index of '" + paramNumber + "' is invalid.");
        }

        if (paramNumber > this.parameterCount) {
            throw new SQLException(
                "Parameter index of '" + paramNumber + "' is greater than number of parameters, which is '" + this.parameterCount + "'.");
        }
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        return null;
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return false;
    }
}
