package io.vitess.util;

import io.vitess.proto.Query;
import java.sql.Connection;
import java.sql.Types;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by ashudeep.sharma on 07/03/16.
 */


/**
 * MysqlDefs contains many values that are needed for communication with the MySQL server.
 */
public final class MysqlDefs {
    public static final int FIELD_TYPE_BLOB = 252;
    /**
     * Used to indicate that the server sent no field-level character set information, so the driver should use the
     * connection-level character encoding instead.
     */
    public static final int NO_CHARSET_INFO = -1;
    static final int COM_BINLOG_DUMP = 18;
    static final int COM_CHANGE_USER = 17;
    static final int COM_CLOSE_STATEMENT = 25;
    static final int COM_CONNECT_OUT = 20;
    static final int COM_END = 29;
    static final int COM_EXECUTE = 23;
    static final int COM_FETCH = 28;
    static final int COM_LONG_DATA = 24;
    static final int COM_PREPARE = 22;
    static final int COM_REGISTER_SLAVE = 21;
    static final int COM_RESET_STMT = 26;
    static final int COM_SET_OPTION = 27;
    static final int COM_TABLE_DUMP = 19;
    static final int CONNECT = 11;
    static final int CREATE_DB = 5;
    static final int DEBUG = 13;
    static final int DELAYED_INSERT = 16;
    static final int DROP_DB = 6;
    static final int FIELD_LIST = 4;
    static final int FIELD_TYPE_BIT = 16;
    static final int FIELD_TYPE_DATE = 10;
    static final int FIELD_TYPE_DATETIME = 12;
    // Data Types
    static final int FIELD_TYPE_DECIMAL = 0;
    static final int FIELD_TYPE_DOUBLE = 5;
    static final int FIELD_TYPE_ENUM = 247;
    static final int FIELD_TYPE_FLOAT = 4;
    static final int FIELD_TYPE_GEOMETRY = 255;
    static final int FIELD_TYPE_INT24 = 9;
    static final int FIELD_TYPE_LONG = 3;
    static final int FIELD_TYPE_LONG_BLOB = 251;
    static final int FIELD_TYPE_LONGLONG = 8;
    static final int FIELD_TYPE_MEDIUM_BLOB = 250;
    static final int FIELD_TYPE_NEW_DECIMAL = 246;
    static final int FIELD_TYPE_NEWDATE = 14;
    static final int FIELD_TYPE_NULL = 6;
    static final int FIELD_TYPE_SET = 248;
    static final int FIELD_TYPE_SHORT = 2;
    static final int FIELD_TYPE_STRING = 254;
    static final int FIELD_TYPE_TIME = 11;
    static final int FIELD_TYPE_TIMESTAMP = 7;
    static final int FIELD_TYPE_TINY = 1;
    // Older data types
    static final int FIELD_TYPE_TINY_BLOB = 249;
    static final int FIELD_TYPE_VAR_STRING = 253;
    static final int FIELD_TYPE_VARCHAR = 15;
    // Newer data types
    static final int FIELD_TYPE_YEAR = 13;
    static final int FIELD_TYPE_JSON = 245;
    static final int INIT_DB = 2;

    // Unlike mysql-vanilla, vtgate returns ints for Field.getLength(). To ensure no type conversion issues,
    // we diverge from mysql-connector-j here, who instead have these fields as longs, and have a function clampedGetLength
    // to convert field lengths to ints after comparison.
    public static final int LENGTH_BLOB = 65535;
    public static final int LENGTH_LONGBLOB = Integer.MAX_VALUE;
    public static final int LENGTH_MEDIUMBLOB = 16777215;
    public static final int LENGTH_TINYBLOB = 255;

    // Limitations
    static final int MAX_ROWS = 50000000; // From the MySQL FAQ
    static final byte OPEN_CURSOR_FLAG = 1;

    static final int PING = 14;

    static final int PROCESS_INFO = 10;

    static final int PROCESS_KILL = 12;

    static final int QUERY = 3;

    static final int QUIT = 1;

    static final int RELOAD = 7;

    static final int SHUTDOWN = 8;

    //
    // Constants defined from mysql
    //
    // DB Operations
    static final int SLEEP = 0;

    static final int STATISTICS = 9;

    static final int TIME = 15;
    public static Map<Query.Type, Integer> vitesstoMySqlType = new HashMap<Query.Type, Integer>();
    public static Map<String, Integer> mysqlConnectionTransactionMapping =
        new HashMap<String, Integer>();
    private static Map<String, Integer> mysqlToJdbcTypesMap = new HashMap<String, Integer>();

    static {
        mysqlToJdbcTypesMap.put("BIT", mysqlToJavaType(FIELD_TYPE_BIT));

        mysqlToJdbcTypesMap.put("TINYINT", mysqlToJavaType(FIELD_TYPE_TINY));
        mysqlToJdbcTypesMap.put("SMALLINT", mysqlToJavaType(FIELD_TYPE_SHORT));
        mysqlToJdbcTypesMap.put("MEDIUMINT", mysqlToJavaType(FIELD_TYPE_INT24));
        mysqlToJdbcTypesMap.put("INT", mysqlToJavaType(FIELD_TYPE_LONG));
        mysqlToJdbcTypesMap.put("INTEGER", mysqlToJavaType(FIELD_TYPE_LONG));
        mysqlToJdbcTypesMap.put("BIGINT", mysqlToJavaType(FIELD_TYPE_LONGLONG));
        mysqlToJdbcTypesMap.put("INT24", mysqlToJavaType(FIELD_TYPE_INT24));
        mysqlToJdbcTypesMap.put("REAL", mysqlToJavaType(FIELD_TYPE_DOUBLE));
        mysqlToJdbcTypesMap.put("FLOAT", mysqlToJavaType(FIELD_TYPE_FLOAT));
        mysqlToJdbcTypesMap.put("DECIMAL", mysqlToJavaType(FIELD_TYPE_DECIMAL));
        mysqlToJdbcTypesMap.put("NUMERIC", mysqlToJavaType(FIELD_TYPE_DECIMAL));
        mysqlToJdbcTypesMap.put("DOUBLE", mysqlToJavaType(FIELD_TYPE_DOUBLE));
        mysqlToJdbcTypesMap.put("CHAR", mysqlToJavaType(FIELD_TYPE_STRING));
        mysqlToJdbcTypesMap.put("VARCHAR", mysqlToJavaType(FIELD_TYPE_VAR_STRING));
        mysqlToJdbcTypesMap.put("DATE", mysqlToJavaType(FIELD_TYPE_DATE));
        mysqlToJdbcTypesMap.put("TIME", mysqlToJavaType(FIELD_TYPE_TIME));
        mysqlToJdbcTypesMap.put("YEAR", mysqlToJavaType(FIELD_TYPE_YEAR));
        mysqlToJdbcTypesMap.put("TIMESTAMP", mysqlToJavaType(FIELD_TYPE_TIMESTAMP));
        mysqlToJdbcTypesMap.put("DATETIME", mysqlToJavaType(FIELD_TYPE_DATETIME));
        mysqlToJdbcTypesMap.put("TINYBLOB", Types.BINARY);
        mysqlToJdbcTypesMap.put("BLOB", Types.LONGVARBINARY);
        mysqlToJdbcTypesMap.put("MEDIUMBLOB", Types.LONGVARBINARY);
        mysqlToJdbcTypesMap.put("LONGBLOB", Types.LONGVARBINARY);
        mysqlToJdbcTypesMap.put("TINYTEXT", Types.VARCHAR);
        mysqlToJdbcTypesMap.put("TEXT", Types.LONGVARCHAR);
        mysqlToJdbcTypesMap.put("MEDIUMTEXT", Types.LONGVARCHAR);
        mysqlToJdbcTypesMap.put("LONGTEXT", Types.LONGVARCHAR);
        mysqlToJdbcTypesMap.put("ENUM", mysqlToJavaType(FIELD_TYPE_ENUM));
        mysqlToJdbcTypesMap.put("SET", mysqlToJavaType(FIELD_TYPE_SET));
        mysqlToJdbcTypesMap.put("GEOMETRY", mysqlToJavaType(FIELD_TYPE_GEOMETRY));
        mysqlToJdbcTypesMap.put("JSON", mysqlToJavaType(FIELD_TYPE_JSON));
    }

    static {
        vitesstoMySqlType.put(Query.Type.NULL_TYPE, Types.NULL);
        vitesstoMySqlType.put(Query.Type.INT8, Types.TINYINT);
        vitesstoMySqlType.put(Query.Type.UINT8, Types.TINYINT);
        vitesstoMySqlType.put(Query.Type.INT16, Types.SMALLINT);
        vitesstoMySqlType.put(Query.Type.UINT16, Types.SMALLINT);
        vitesstoMySqlType.put(Query.Type.INT24, Types.INTEGER);
        vitesstoMySqlType.put(Query.Type.UINT24, Types.INTEGER);
        vitesstoMySqlType.put(Query.Type.INT32, Types.INTEGER);
        vitesstoMySqlType.put(Query.Type.UINT32, Types.INTEGER);
        vitesstoMySqlType.put(Query.Type.INT64, Types.BIGINT);
        vitesstoMySqlType.put(Query.Type.UINT64, Types.BIGINT);
        vitesstoMySqlType.put(Query.Type.FLOAT32, Types.FLOAT);
        vitesstoMySqlType.put(Query.Type.FLOAT64, Types.DOUBLE);
        vitesstoMySqlType.put(Query.Type.TIMESTAMP, Types.TIMESTAMP);
        vitesstoMySqlType.put(Query.Type.DATE, Types.DATE);
        vitesstoMySqlType.put(Query.Type.TIME, Types.TIME);
        vitesstoMySqlType.put(Query.Type.DATETIME, Types.TIMESTAMP);
        vitesstoMySqlType.put(Query.Type.YEAR, Types.SMALLINT);
        vitesstoMySqlType.put(Query.Type.DECIMAL, Types.DECIMAL);
        vitesstoMySqlType.put(Query.Type.TEXT, Types.VARCHAR);
        vitesstoMySqlType.put(Query.Type.BLOB, Types.BLOB);
        vitesstoMySqlType.put(Query.Type.VARCHAR, Types.VARCHAR);
        vitesstoMySqlType.put(Query.Type.VARBINARY, Types.VARBINARY);
        vitesstoMySqlType.put(Query.Type.CHAR, Types.CHAR);
        vitesstoMySqlType.put(Query.Type.BINARY, Types.BINARY);
        vitesstoMySqlType.put(Query.Type.BIT, Types.BIT);
        vitesstoMySqlType.put(Query.Type.ENUM, Types.CHAR);
        vitesstoMySqlType.put(Query.Type.SET, Types.CHAR);
        vitesstoMySqlType.put(Query.Type.TUPLE, Types.OTHER);
        vitesstoMySqlType.put(Query.Type.GEOMETRY, Types.BINARY);
        vitesstoMySqlType.put(Query.Type.JSON, Types.BINARY);
    }

    static {
        mysqlConnectionTransactionMapping.put("TRANSACTION-NONE", Connection.TRANSACTION_NONE);
        mysqlConnectionTransactionMapping
            .put("REPEATABLE-READ", Connection.TRANSACTION_REPEATABLE_READ);
        mysqlConnectionTransactionMapping
            .put("READ-COMMITTED", Connection.TRANSACTION_READ_COMMITTED);
        mysqlConnectionTransactionMapping
            .put("READ-UNCOMMITTED", Connection.TRANSACTION_READ_UNCOMMITTED);
        mysqlConnectionTransactionMapping.put("SERIALIZABLE", Connection.TRANSACTION_SERIALIZABLE);
    }

    /**
     * Maps the given MySQL type to the correct JDBC type.
     */
    public static int mysqlToJavaType(int mysqlType) {
        int jdbcType;

        switch (mysqlType) {
            case MysqlDefs.FIELD_TYPE_NEW_DECIMAL:
            case MysqlDefs.FIELD_TYPE_DECIMAL:
                jdbcType = Types.DECIMAL;

                break;

            case MysqlDefs.FIELD_TYPE_TINY:
                jdbcType = Types.TINYINT;

                break;

            case MysqlDefs.FIELD_TYPE_SHORT:
                jdbcType = Types.SMALLINT;

                break;

            case MysqlDefs.FIELD_TYPE_LONG:
                jdbcType = Types.INTEGER;

                break;

            case MysqlDefs.FIELD_TYPE_FLOAT:
                jdbcType = Types.REAL;

                break;

            case MysqlDefs.FIELD_TYPE_DOUBLE:
                jdbcType = Types.DOUBLE;

                break;

            case MysqlDefs.FIELD_TYPE_NULL:
                jdbcType = Types.NULL;

                break;

            case MysqlDefs.FIELD_TYPE_TIMESTAMP:
                jdbcType = Types.TIMESTAMP;

                break;

            case MysqlDefs.FIELD_TYPE_LONGLONG:
                jdbcType = Types.BIGINT;

                break;

            case MysqlDefs.FIELD_TYPE_INT24:
                jdbcType = Types.INTEGER;

                break;

            case MysqlDefs.FIELD_TYPE_DATE:
                jdbcType = Types.DATE;

                break;

            case MysqlDefs.FIELD_TYPE_TIME:
                jdbcType = Types.TIME;

                break;

            case MysqlDefs.FIELD_TYPE_DATETIME:
                jdbcType = Types.TIMESTAMP;

                break;

            case MysqlDefs.FIELD_TYPE_YEAR:
                jdbcType = Types.DATE;

                break;

            case MysqlDefs.FIELD_TYPE_NEWDATE:
                jdbcType = Types.DATE;

                break;

            case MysqlDefs.FIELD_TYPE_ENUM:
                jdbcType = Types.CHAR;

                break;

            case MysqlDefs.FIELD_TYPE_SET:
                jdbcType = Types.CHAR;

                break;

            case MysqlDefs.FIELD_TYPE_TINY_BLOB:
                jdbcType = Types.VARBINARY;

                break;

            case MysqlDefs.FIELD_TYPE_MEDIUM_BLOB:
                jdbcType = Types.LONGVARBINARY;

                break;

            case MysqlDefs.FIELD_TYPE_LONG_BLOB:
                jdbcType = Types.LONGVARBINARY;

                break;

            case MysqlDefs.FIELD_TYPE_BLOB:
                jdbcType = Types.LONGVARBINARY;

                break;

            case MysqlDefs.FIELD_TYPE_VAR_STRING:
            case MysqlDefs.FIELD_TYPE_VARCHAR:
                jdbcType = Types.VARCHAR;

                break;

            case MysqlDefs.FIELD_TYPE_JSON:
            case MysqlDefs.FIELD_TYPE_STRING:
                jdbcType = Types.CHAR;

                break;
            case MysqlDefs.FIELD_TYPE_GEOMETRY:
                jdbcType = Types.BINARY;

                break;
            case MysqlDefs.FIELD_TYPE_BIT:
                jdbcType = Types.BIT;

                break;
            default:
                jdbcType = Types.VARCHAR;
        }

        return jdbcType;
    }

    /**
     * Maps the given MySQL type to the correct JDBC type.
     */
    public static int mysqlToJavaType(String mysqlType) {
        if (mysqlType.equalsIgnoreCase("BIT")) {
            return mysqlToJavaType(FIELD_TYPE_BIT);
        } else if (mysqlType.equalsIgnoreCase("TINYINT")) {
            return mysqlToJavaType(FIELD_TYPE_TINY);
        } else if (mysqlType.equalsIgnoreCase("SMALLINT")) {
            return mysqlToJavaType(FIELD_TYPE_SHORT);
        } else if (mysqlType.equalsIgnoreCase("MEDIUMINT")) {
            return mysqlToJavaType(FIELD_TYPE_INT24);
        } else if (mysqlType.equalsIgnoreCase("INT") || mysqlType.equalsIgnoreCase("INTEGER")) {
            return mysqlToJavaType(FIELD_TYPE_LONG);
        } else if (mysqlType.equalsIgnoreCase("BIGINT")) {
            return mysqlToJavaType(FIELD_TYPE_LONGLONG);
        } else if (mysqlType.equalsIgnoreCase("INT24")) {
            return mysqlToJavaType(FIELD_TYPE_INT24);
        } else if (mysqlType.equalsIgnoreCase("REAL")) {
            return mysqlToJavaType(FIELD_TYPE_DOUBLE);
        } else if (mysqlType.equalsIgnoreCase("FLOAT")) {
            return mysqlToJavaType(FIELD_TYPE_FLOAT);
        } else if (mysqlType.equalsIgnoreCase("DECIMAL")) {
            return mysqlToJavaType(FIELD_TYPE_DECIMAL);
        } else if (mysqlType.equalsIgnoreCase("NUMERIC")) {
            return mysqlToJavaType(FIELD_TYPE_DECIMAL);
        } else if (mysqlType.equalsIgnoreCase("DOUBLE")) {
            return mysqlToJavaType(FIELD_TYPE_DOUBLE);
        } else if (mysqlType.equalsIgnoreCase("CHAR")) {
            return mysqlToJavaType(FIELD_TYPE_STRING);
        } else if (mysqlType.equalsIgnoreCase("VARCHAR")) {
            return mysqlToJavaType(FIELD_TYPE_VAR_STRING);
        } else if (mysqlType.equalsIgnoreCase("DATE")) {
            return mysqlToJavaType(FIELD_TYPE_DATE);
        } else if (mysqlType.equalsIgnoreCase("TIME")) {
            return mysqlToJavaType(FIELD_TYPE_TIME);
        } else if (mysqlType.equalsIgnoreCase("YEAR")) {
            return mysqlToJavaType(FIELD_TYPE_YEAR);
        } else if (mysqlType.equalsIgnoreCase("TIMESTAMP")) {
            return mysqlToJavaType(FIELD_TYPE_TIMESTAMP);
        } else if (mysqlType.equalsIgnoreCase("DATETIME")) {
            return mysqlToJavaType(FIELD_TYPE_DATETIME);
        } else if (mysqlType.equalsIgnoreCase("TINYBLOB")) {
            return Types.BINARY;
        } else if (mysqlType.equalsIgnoreCase("BLOB")) {
            return Types.LONGVARBINARY;
        } else if (mysqlType.equalsIgnoreCase("MEDIUMBLOB")) {
            return Types.LONGVARBINARY;
        } else if (mysqlType.equalsIgnoreCase("LONGBLOB")) {
            return Types.LONGVARBINARY;
        } else if (mysqlType.equalsIgnoreCase("TINYTEXT")) {
            return Types.VARCHAR;
        } else if (mysqlType.equalsIgnoreCase("TEXT")) {
            return Types.LONGVARCHAR;
        } else if (mysqlType.equalsIgnoreCase("MEDIUMTEXT")) {
            return Types.LONGVARCHAR;
        } else if (mysqlType.equalsIgnoreCase("LONGTEXT")) {
            return Types.LONGVARCHAR;
        } else if (mysqlType.equalsIgnoreCase("ENUM")) {
            return mysqlToJavaType(FIELD_TYPE_ENUM);
        } else if (mysqlType.equalsIgnoreCase("SET")) {
            return mysqlToJavaType(FIELD_TYPE_SET);
        } else if (mysqlType.equalsIgnoreCase("GEOMETRY")) {
            return mysqlToJavaType(FIELD_TYPE_GEOMETRY);
        } else if (mysqlType.equalsIgnoreCase("BINARY")) {
            return Types.BINARY; // no concrete type on the wire
        } else if (mysqlType.equalsIgnoreCase("VARBINARY")) {
            return Types.VARBINARY; // no concrete type on the wire
        } else if (mysqlType.equalsIgnoreCase("BIT")) {
            return mysqlToJavaType(FIELD_TYPE_BIT);
        } else if (mysqlType.equalsIgnoreCase("JSON")) {
            return mysqlToJavaType(FIELD_TYPE_JSON);
        }

        // Punt
        return Types.OTHER;
    }

    /**
     * @param mysqlType
     */
    public static String typeToName(int mysqlType) {
        switch (mysqlType) {
            case MysqlDefs.FIELD_TYPE_DECIMAL:
                return "FIELD_TYPE_DECIMAL";

            case MysqlDefs.FIELD_TYPE_TINY:
                return "FIELD_TYPE_TINY";

            case MysqlDefs.FIELD_TYPE_SHORT:
                return "FIELD_TYPE_SHORT";

            case MysqlDefs.FIELD_TYPE_LONG:
                return "FIELD_TYPE_LONG";

            case MysqlDefs.FIELD_TYPE_FLOAT:
                return "FIELD_TYPE_FLOAT";

            case MysqlDefs.FIELD_TYPE_DOUBLE:
                return "FIELD_TYPE_DOUBLE";

            case MysqlDefs.FIELD_TYPE_NULL:
                return "FIELD_TYPE_NULL";

            case MysqlDefs.FIELD_TYPE_TIMESTAMP:
                return "FIELD_TYPE_TIMESTAMP";

            case MysqlDefs.FIELD_TYPE_LONGLONG:
                return "FIELD_TYPE_LONGLONG";

            case MysqlDefs.FIELD_TYPE_INT24:
                return "FIELD_TYPE_INT24";

            case MysqlDefs.FIELD_TYPE_DATE:
                return "FIELD_TYPE_DATE";

            case MysqlDefs.FIELD_TYPE_TIME:
                return "FIELD_TYPE_TIME";

            case MysqlDefs.FIELD_TYPE_DATETIME:
                return "FIELD_TYPE_DATETIME";

            case MysqlDefs.FIELD_TYPE_YEAR:
                return "FIELD_TYPE_YEAR";

            case MysqlDefs.FIELD_TYPE_NEWDATE:
                return "FIELD_TYPE_NEWDATE";

            case MysqlDefs.FIELD_TYPE_ENUM:
                return "FIELD_TYPE_ENUM";

            case MysqlDefs.FIELD_TYPE_SET:
                return "FIELD_TYPE_SET";

            case MysqlDefs.FIELD_TYPE_TINY_BLOB:
                return "FIELD_TYPE_TINY_BLOB";

            case MysqlDefs.FIELD_TYPE_MEDIUM_BLOB:
                return "FIELD_TYPE_MEDIUM_BLOB";

            case MysqlDefs.FIELD_TYPE_LONG_BLOB:
                return "FIELD_TYPE_LONG_BLOB";

            case MysqlDefs.FIELD_TYPE_BLOB:
                return "FIELD_TYPE_BLOB";

            case MysqlDefs.FIELD_TYPE_VAR_STRING:
                return "FIELD_TYPE_VAR_STRING";

            case MysqlDefs.FIELD_TYPE_STRING:
                return "FIELD_TYPE_STRING";

            case MysqlDefs.FIELD_TYPE_VARCHAR:
                return "FIELD_TYPE_VARCHAR";

            case MysqlDefs.FIELD_TYPE_GEOMETRY:
                return "FIELD_TYPE_GEOMETRY";

            case MysqlDefs.FIELD_TYPE_JSON:
                return "FIELD_TYPE_JSON";

            default:
                return " Unknown MySQL Type # " + mysqlType;
        }
    }

    static void appendJdbcTypeMappingQuery(StringBuilder buf, String mysqlTypeColumnName) {

        buf.append("CASE ");
        Map<String, Integer> typesMap = new HashMap<String, Integer>();
        typesMap.putAll(mysqlToJdbcTypesMap);
        typesMap.put("BINARY", Types.BINARY);
        typesMap.put("VARBINARY", Types.VARBINARY);

        for (String mysqlTypeName : typesMap.keySet()) {
            buf.append(" WHEN ");
            buf.append(mysqlTypeColumnName);
            buf.append("='");
            buf.append(mysqlTypeName);
            buf.append("' THEN ");
            buf.append(typesMap.get(mysqlTypeName));

            if (mysqlTypeName.equalsIgnoreCase("DOUBLE") || mysqlTypeName.equalsIgnoreCase("FLOAT")
                || mysqlTypeName.equalsIgnoreCase("DECIMAL") || mysqlTypeName
                .equalsIgnoreCase("NUMERIC")) {
                buf.append(" WHEN ");
                buf.append(mysqlTypeColumnName);
                buf.append("='");
                buf.append(mysqlTypeName);
                buf.append(" unsigned' THEN ");
                buf.append(typesMap.get(mysqlTypeName));
            }
        }

        buf.append(" ELSE ");
        buf.append(Types.OTHER);
        buf.append(" END ");
    }
}
