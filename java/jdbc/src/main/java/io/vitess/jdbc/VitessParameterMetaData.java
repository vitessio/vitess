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
