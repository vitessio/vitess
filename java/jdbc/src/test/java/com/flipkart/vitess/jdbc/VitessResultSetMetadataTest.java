package com.flipkart.vitess.jdbc;

import com.flipkart.vitess.util.Constants;
import com.youtube.vitess.proto.Query;

import org.junit.Assert;
import org.junit.Test;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by ashudeep.sharma on 08/02/16.
 */
public class VitessResultSetMetadataTest extends BaseTest {

    private List<FieldWithMetadata> fieldList;

    private List<Query.Field> generateFieldList() {
        List<Query.Field> fieldList = new ArrayList<>();

        fieldList.add(field("col1", "tbl", Query.Type.INT8, 4)
            .setFlags(Query.MySqlFlag.NOT_NULL_FLAG_VALUE).setOrgName("foo").setOrgTable("foo").build());
        fieldList.add(field("col2", "tbl", Query.Type.UINT8, 3).setFlags(Query.MySqlFlag.UNSIGNED_FLAG_VALUE).build());
        fieldList.add(field("col3", "tbl", Query.Type.INT16, 6).build());
        fieldList.add(field("col4", "tbl", Query.Type.UINT16, 5).setFlags(Query.MySqlFlag.UNSIGNED_FLAG_VALUE).build());
        fieldList.add(field("col5", "tbl", Query.Type.INT24, 9).build());
        fieldList.add(field("col6", "tbl", Query.Type.UINT24, 8).setFlags(Query.MySqlFlag.UNSIGNED_FLAG_VALUE).build());
        fieldList.add(field("col7", "tbl", Query.Type.INT32, 11).build());
        fieldList.add(field("col8", "tbl", Query.Type.UINT32, 10).setFlags(Query.MySqlFlag.UNSIGNED_FLAG_VALUE).build());
        fieldList.add(field("col9", "tbl", Query.Type.INT64, 20).build());
        fieldList.add(field("col10", "tbl", Query.Type.UINT64, 20).setFlags(Query.MySqlFlag.UNSIGNED_FLAG_VALUE).build());
        fieldList.add(field("col11", "tbl", Query.Type.FLOAT32, 12).setDecimals(31).build());
        fieldList.add(field("col12", "tbl", Query.Type.FLOAT64, 22).setDecimals(31).build());
        fieldList.add(field("col13", "tbl", Query.Type.TIMESTAMP, 10).build());
        fieldList.add(field("col14", "tbl", Query.Type.DATE, 10).build());
        fieldList.add(field("col15", "tbl", Query.Type.TIME, 10).build());
        fieldList.add(field("col16", "tbl", Query.Type.DATETIME, 19).build());
        fieldList.add(field("col17", "tbl", Query.Type.YEAR, 4).build());
        fieldList.add(field("col18", "tbl", Query.Type.DECIMAL, 7).setDecimals(2).build());
        fieldList.add(field("col19", "tbl", Query.Type.TEXT, 765).build());
        fieldList.add(field("col20", "tbl", Query.Type.BLOB, 65535)
            .setFlags(Query.MySqlFlag.BINARY_FLAG_VALUE | Query.MySqlFlag.BLOB_FLAG_VALUE)
            .setDecimals(/* this is set to facilitate testing of getScale, since this is non-numeric */2).build());
        fieldList.add(field("col21", "tbl", Query.Type.VARCHAR, 768).build());
        fieldList.add(field("col22", "tbl", Query.Type.VARBINARY, 256).setFlags(Query.MySqlFlag.BINARY_FLAG_VALUE).build());
        fieldList.add(field("col23", "tbl", Query.Type.CHAR, 48).build());
        fieldList.add(field("col24", "tbl", Query.Type.BINARY, 4).setFlags(Query.MySqlFlag.BINARY_FLAG_VALUE).build());
        fieldList.add(field("col25", "tbl", Query.Type.BIT, 8).build());
        fieldList.add(field("col26", "tbl", Query.Type.ENUM, 3).build());
        fieldList.add(field("col27", "tbl", Query.Type.SET, 9).build());
        fieldList.add(field("col28", "tbl", Query.Type.TUPLE, 0).build());
        fieldList.add(field("col29", "tbl", Query.Type.VARBINARY, 256).setFlags(Query.MySqlFlag.BINARY_FLAG_VALUE).build());
        fieldList.add(field("col30", "tbl", Query.Type.BLOB, 65535)
            .setFlags(Query.MySqlFlag.BLOB_FLAG_VALUE).build());
        return fieldList;
    }

    private Query.Field.Builder field(String name, String table, Query.Type type, int length) {
        return Query.Field.newBuilder()
            .setName(name)
            .setTable(table)
            .setType(type)
            .setColumnLength(length);
    }

    private void initializeFieldList(VitessConnection connection) throws SQLException {
        List<Query.Field> fields = generateFieldList();
        this.fieldList = new ArrayList<>(fields.size());
        for (Query.Field field : fields) {
            this.fieldList.add(new FieldWithMetadata(connection, field));
        }
    }

    public List<FieldWithMetadata> getFieldList() throws SQLException {
        return getFieldList(getVitessConnection());
    }

    public List<FieldWithMetadata> getFieldList(VitessConnection conn) throws SQLException {
        initializeFieldList(conn);
        return this.fieldList;
    }

    @Test public void testgetColumnCount() throws SQLException {

        List<FieldWithMetadata> fieldList = getFieldList();
        VitessResultSetMetaData vitessResultSetMetadata = new VitessResultSetMetaData(fieldList);
        Assert.assertEquals(30, vitessResultSetMetadata.getColumnCount());
    }

    @Test public void testgetColumnName() throws SQLException {

        List<FieldWithMetadata> fieldList = getFieldList();
        VitessResultSetMetaData vitessResultSetMetadata = new VitessResultSetMetaData(fieldList);
        for (int i = 1; i <= vitessResultSetMetadata.getColumnCount(); i++) {
            Assert.assertEquals("col" + i, vitessResultSetMetadata.getColumnName(i));
        }
    }

    @Test public void testgetColumnTypeName() throws SQLException {

        List<FieldWithMetadata> fieldList = getFieldList();
        VitessResultSetMetaData vitessResultSetMetadata = new VitessResultSetMetaData(fieldList);
        Assert.assertEquals("TINYINT", vitessResultSetMetadata.getColumnTypeName(1));
        Assert.assertEquals("TINYINT UNSIGNED", vitessResultSetMetadata.getColumnTypeName(2));
        Assert.assertEquals("SMALLINT", vitessResultSetMetadata.getColumnTypeName(3));
        Assert.assertEquals("SMALLINT UNSIGNED", vitessResultSetMetadata.getColumnTypeName(4));
        Assert.assertEquals("MEDIUMINT", vitessResultSetMetadata.getColumnTypeName(5));
        Assert.assertEquals("MEDIUMINT UNSIGNED", vitessResultSetMetadata.getColumnTypeName(6));
        Assert.assertEquals("INT", vitessResultSetMetadata.getColumnTypeName(7));
        Assert.assertEquals("INT UNSIGNED", vitessResultSetMetadata.getColumnTypeName(8));
        Assert.assertEquals("BIGINT", vitessResultSetMetadata.getColumnTypeName(9));
        Assert.assertEquals("BIGINT UNSIGNED", vitessResultSetMetadata.getColumnTypeName(10));
        Assert.assertEquals("FLOAT", vitessResultSetMetadata.getColumnTypeName(11));
        Assert.assertEquals("DOUBLE", vitessResultSetMetadata.getColumnTypeName(12));
        Assert.assertEquals("TIMESTAMP", vitessResultSetMetadata.getColumnTypeName(13));
        Assert.assertEquals("DATE", vitessResultSetMetadata.getColumnTypeName(14));
        Assert.assertEquals("TIME", vitessResultSetMetadata.getColumnTypeName(15));
        Assert.assertEquals("DATETIME", vitessResultSetMetadata.getColumnTypeName(16));
        Assert.assertEquals("YEAR", vitessResultSetMetadata.getColumnTypeName(17));
        Assert.assertEquals("DECIMAL", vitessResultSetMetadata.getColumnTypeName(18));
        Assert.assertEquals("TEXT", vitessResultSetMetadata.getColumnTypeName(19));
        Assert.assertEquals("BLOB", vitessResultSetMetadata.getColumnTypeName(20));
        Assert.assertEquals("VARCHAR", vitessResultSetMetadata.getColumnTypeName(21));
        Assert.assertEquals("VARBINARY", vitessResultSetMetadata.getColumnTypeName(22));
        Assert.assertEquals("CHAR", vitessResultSetMetadata.getColumnTypeName(23));
        Assert.assertEquals("BINARY", vitessResultSetMetadata.getColumnTypeName(24));
        Assert.assertEquals("BIT", vitessResultSetMetadata.getColumnTypeName(25));
        Assert.assertEquals("ENUM", vitessResultSetMetadata.getColumnTypeName(26));
        Assert.assertEquals("SET", vitessResultSetMetadata.getColumnTypeName(27));
        Assert.assertEquals("TUPLE", vitessResultSetMetadata.getColumnTypeName(28));
        Assert.assertEquals("VARBINARY", vitessResultSetMetadata.getColumnTypeName(29));
        Assert.assertEquals("BLOB", vitessResultSetMetadata.getColumnTypeName(30));
    }

    @Test public void testgetColumnType() throws SQLException {

        List<FieldWithMetadata> fieldList = getFieldList();
        VitessResultSetMetaData vitessResultSetMetadata = new VitessResultSetMetaData(fieldList);
        Assert.assertEquals("TINYINT", Types.TINYINT, vitessResultSetMetadata.getColumnType(1));
        Assert.assertEquals("TINYINT", Types.TINYINT, vitessResultSetMetadata.getColumnType(2));
        Assert.assertEquals("SMALLINT", Types.SMALLINT, vitessResultSetMetadata.getColumnType(3));
        Assert.assertEquals("SMALLINT", Types.SMALLINT, vitessResultSetMetadata.getColumnType(4));
        Assert.assertEquals("INTEGER", Types.INTEGER, vitessResultSetMetadata.getColumnType(5));
        Assert.assertEquals("INTEGER", Types.INTEGER, vitessResultSetMetadata.getColumnType(6));
        Assert.assertEquals("INTEGER", Types.INTEGER, vitessResultSetMetadata.getColumnType(7));
        Assert.assertEquals("INTEGER", Types.INTEGER, vitessResultSetMetadata.getColumnType(8));
        Assert.assertEquals("BIGINT", Types.BIGINT, vitessResultSetMetadata.getColumnType(9));
        Assert.assertEquals("BIGINT", Types.BIGINT, vitessResultSetMetadata.getColumnType(10));
        Assert.assertEquals("FLOAT", Types.FLOAT, vitessResultSetMetadata.getColumnType(11));
        Assert.assertEquals("DOUBLE", Types.DOUBLE, vitessResultSetMetadata.getColumnType(12));
        Assert.assertEquals("TIMESTAMP", Types.TIMESTAMP, vitessResultSetMetadata.getColumnType(13));
        Assert.assertEquals("DATE", Types.DATE, vitessResultSetMetadata.getColumnType(14));
        Assert.assertEquals("TIME", Types.TIME, vitessResultSetMetadata.getColumnType(15));
        Assert.assertEquals("TIMESTAMP", Types.TIMESTAMP, vitessResultSetMetadata.getColumnType(16));
        Assert.assertEquals("SMALLINT", Types.SMALLINT, vitessResultSetMetadata.getColumnType(17));
        Assert.assertEquals("DECIMAL", Types.DECIMAL, vitessResultSetMetadata.getColumnType(18));
        Assert.assertEquals("VARCHAR", Types.VARCHAR, vitessResultSetMetadata.getColumnType(19));
        Assert.assertEquals("BLOB", Types.BLOB, vitessResultSetMetadata.getColumnType(20));
        Assert.assertEquals("VARCHAR", Types.VARCHAR, vitessResultSetMetadata.getColumnType(21));
        Assert.assertEquals("VARBINARY", Types.VARBINARY, vitessResultSetMetadata.getColumnType(22));
        Assert.assertEquals("CHAR", Types.CHAR, vitessResultSetMetadata.getColumnType(23));
        Assert.assertEquals("BINARY", Types.BINARY, vitessResultSetMetadata.getColumnType(24));
        Assert.assertEquals("BIT", Types.BIT, vitessResultSetMetadata.getColumnType(25));
        Assert.assertEquals("CHAR", Types.CHAR, vitessResultSetMetadata.getColumnType(26));
        Assert.assertEquals("CHAR", Types.CHAR, vitessResultSetMetadata.getColumnType(27));
        Assert.assertEquals("VARBINARY", Types.VARBINARY, vitessResultSetMetadata.getColumnType(29));
        Assert.assertEquals("BLOB", Types.BLOB, vitessResultSetMetadata.getColumnType(30));
        try {
            int type = vitessResultSetMetadata.getColumnType(28);
        } catch (SQLException ex) {
            Assert
                .assertEquals(Constants.SQLExceptionMessages.INVALID_COLUMN_TYPE, ex.getMessage());
        }

        try {
            int type = vitessResultSetMetadata.getColumnType(0);
        } catch (SQLException ex) {
            Assert.assertEquals(Constants.SQLExceptionMessages.INVALID_COLUMN_INDEX + ": " + 0,
                ex.getMessage());
        }
    }

    @Test public void testgetColumnLabel() throws SQLException {
        List<FieldWithMetadata> fieldList = getFieldList();
        VitessResultSetMetaData vitessResultSetMetaData = new VitessResultSetMetaData(fieldList);
        for (int i = 1; i <= vitessResultSetMetaData.getColumnCount(); i++) {
            Assert.assertEquals("col" + i, vitessResultSetMetaData.getColumnLabel(i));
        }
    }

    @Test public void testgetTableName() throws SQLException {
        List<FieldWithMetadata> fieldList = getFieldList();
        VitessResultSetMetaData vitessResultSetMetadata = new VitessResultSetMetaData(fieldList);
        for (int i = 1; i <= vitessResultSetMetadata.getColumnCount(); i++) {
            Assert.assertEquals(vitessResultSetMetadata.getTableName(i), "tbl");
        }
    }

    @Test public void isReadOnlyTest() throws SQLException {
        List<FieldWithMetadata> fieldList = getFieldList();
        VitessResultSetMetaData vitessResultSetMetadata = new VitessResultSetMetaData(fieldList);
        Assert.assertEquals(vitessResultSetMetadata.isReadOnly(1), false);
        Assert.assertEquals(vitessResultSetMetadata.isWritable(1), true);
        Assert.assertEquals(vitessResultSetMetadata.isDefinitelyWritable(1), true);

        for (int i = 2; i <= vitessResultSetMetadata.getColumnCount(); i++) {
            Assert.assertEquals(vitessResultSetMetadata.isReadOnly(i), true);
            Assert.assertEquals(vitessResultSetMetadata.isWritable(i), false);
            Assert.assertEquals(vitessResultSetMetadata.isDefinitelyWritable(i), false);
        }
    }

    @Test public void getColumnTypeNameTest() throws SQLException {
        List<FieldWithMetadata> fieldList = getFieldList();
        VitessResultSetMetaData vitessResultSetMetadata = new VitessResultSetMetaData(fieldList);
        Assert.assertEquals("TINYINT", vitessResultSetMetadata.getColumnTypeName(1));
        Assert.assertEquals("TINYINT UNSIGNED", vitessResultSetMetadata.getColumnTypeName(2));
        Assert.assertEquals("SMALLINT", vitessResultSetMetadata.getColumnTypeName(3));
        Assert.assertEquals("SMALLINT UNSIGNED", vitessResultSetMetadata.getColumnTypeName(4));
        Assert.assertEquals("MEDIUMINT", vitessResultSetMetadata.getColumnTypeName(5));
        Assert.assertEquals("MEDIUMINT UNSIGNED", vitessResultSetMetadata.getColumnTypeName(6));
        Assert.assertEquals("INT", vitessResultSetMetadata.getColumnTypeName(7));
        Assert.assertEquals("INT UNSIGNED", vitessResultSetMetadata.getColumnTypeName(8));
        Assert.assertEquals("BIGINT", vitessResultSetMetadata.getColumnTypeName(9));
        Assert.assertEquals("BIGINT UNSIGNED", vitessResultSetMetadata.getColumnTypeName(10));
        Assert.assertEquals("FLOAT", vitessResultSetMetadata.getColumnTypeName(11));
        Assert.assertEquals("DOUBLE", vitessResultSetMetadata.getColumnTypeName(12));
        Assert.assertEquals("TIMESTAMP", vitessResultSetMetadata.getColumnTypeName(13));
        Assert.assertEquals("DATE", vitessResultSetMetadata.getColumnTypeName(14));
        Assert.assertEquals("TIME", vitessResultSetMetadata.getColumnTypeName(15));
        Assert.assertEquals("DATETIME", vitessResultSetMetadata.getColumnTypeName(16));
        Assert.assertEquals("YEAR", vitessResultSetMetadata.getColumnTypeName(17));
        Assert.assertEquals("DECIMAL", vitessResultSetMetadata.getColumnTypeName(18));
        Assert.assertEquals("TEXT", vitessResultSetMetadata.getColumnTypeName(19));
        Assert.assertEquals("BLOB", vitessResultSetMetadata.getColumnTypeName(20));
        Assert.assertEquals("VARCHAR", vitessResultSetMetadata.getColumnTypeName(21));
        Assert.assertEquals("VARBINARY", vitessResultSetMetadata.getColumnTypeName(22));
        Assert.assertEquals("CHAR", vitessResultSetMetadata.getColumnTypeName(23));
        Assert.assertEquals("BINARY", vitessResultSetMetadata.getColumnTypeName(24));
        Assert.assertEquals("BIT", vitessResultSetMetadata.getColumnTypeName(25));
        Assert.assertEquals("ENUM", vitessResultSetMetadata.getColumnTypeName(26));
        Assert.assertEquals("SET", vitessResultSetMetadata.getColumnTypeName(27));
        Assert.assertEquals("TUPLE", vitessResultSetMetadata.getColumnTypeName(28));
    }

    @Test public void getSchemaNameTest() throws SQLException {
        List<FieldWithMetadata> fieldList = getFieldList();
        VitessResultSetMetaData vitessResultSetMetaData = new VitessResultSetMetaData(fieldList);
        Assert.assertEquals(vitessResultSetMetaData.getSchemaName(1), "");
        Assert.assertEquals(vitessResultSetMetaData.getCatalogName(1), "");
        Assert.assertEquals(vitessResultSetMetaData.getPrecision(1), 0);
        Assert.assertEquals(vitessResultSetMetaData.getScale(1), 0);
        Assert.assertEquals(vitessResultSetMetaData.getColumnDisplaySize(1), 0);
        Assert.assertEquals(vitessResultSetMetaData.isCurrency(1), false);
    }

    @Test public void testCaseSensitivity() throws SQLException {
        VitessConnection connection = getVitessConnection();
        List<FieldWithMetadata> fieldList = getFieldList(connection);
        VitessResultSetMetaData md = new VitessResultSetMetaData(fieldList);

        // numeric types and date types are not case sensitive
        Assert.assertEquals("int8 case sensitivity", false, md.isCaseSensitive(1));
        Assert.assertEquals("uint8 case sensitivity", false, md.isCaseSensitive(2));
        Assert.assertEquals("int16 case sensitivity", false, md.isCaseSensitive(3));
        Assert.assertEquals("uint16 case sensitivity", false, md.isCaseSensitive(4));
        Assert.assertEquals("int24 case sensitivity", false, md.isCaseSensitive(5));
        Assert.assertEquals("uint24 case sensitivity", false, md.isCaseSensitive(6));
        Assert.assertEquals("int32 case sensitivity", false, md.isCaseSensitive(7));
        Assert.assertEquals("uint32 case sensitivity", false, md.isCaseSensitive(8));
        Assert.assertEquals("int64 case sensitivity", false, md.isCaseSensitive(9));
        Assert.assertEquals("uint64 case sensitivity", false, md.isCaseSensitive(10));
        Assert.assertEquals("float32 case sensitivity", false, md.isCaseSensitive(11));
        Assert.assertEquals("float64 case sensitivity", false, md.isCaseSensitive(12));
        Assert.assertEquals("timestamp case sensitivity", false, md.isCaseSensitive(13));
        Assert.assertEquals("date case sensitivity", false, md.isCaseSensitive(14));
        Assert.assertEquals("time case sensitivity", false, md.isCaseSensitive(15));
        Assert.assertEquals("datetime case sensitivity", false, md.isCaseSensitive(16));
        Assert.assertEquals("year case sensitivity", false, md.isCaseSensitive(17));
        Assert.assertEquals("decimal case sensitivity", false, md.isCaseSensitive(18));
        for (int i = 18; i < fieldList.size(); i++) {
            Assert.assertEquals(fieldList.get(i).getName() + " - non-numeric case insensitive", i != 24, md.isCaseSensitive(i + 1));
        }

        connection.setIncludedFields(Query.ExecuteOptions.IncludedFields.TYPE_AND_NAME);
        // with limited included fields, we can really only know about numeric types -- those should return false.  the rest should return true
        for (int i = 0; i < fieldList.size(); i++) {
            Assert.assertEquals(fieldList.get(i).getName() + " - non-numeric case insensitive due to lack of included fields", i >= 18 && i != 24, md.isCaseSensitive(i + 1));
        }
    }

    @Test public void testIsNullable() throws SQLException {
        VitessConnection conn = getVitessConnection();
        List<FieldWithMetadata> fieldList = getFieldList(conn);
        VitessResultSetMetaData md = new VitessResultSetMetaData(fieldList);
        Assert.assertEquals("NOT_NULL flag means columnNoNulls (0) value for isNullable", ResultSetMetaData.columnNoNulls, md.isNullable(1));
        for (int i = 1; i < fieldList.size(); i++) {
            Assert.assertEquals("lack of NOT_NULL flag means columnNullable (1) value for isNullable", ResultSetMetaData.columnNullable, md.isNullable(i + 1));
        }

        conn.setIncludedFields(Query.ExecuteOptions.IncludedFields.TYPE_AND_NAME);
        for (int i = 0; i < fieldList.size(); i++) {
            Assert.assertEquals(fieldList.get(i).getName() + " - isNullable is columnNullableUnknown (2) for all when lack of included fields", 2, md.isNullable(i + 1));
        }
    }

    @Test public void testGetScale() throws SQLException {
        VitessConnection conn = getVitessConnection();
        List<FieldWithMetadata> fieldList = getFieldList(conn);
        VitessResultSetMetaData md = new VitessResultSetMetaData(fieldList);
        Assert.assertEquals("int8 precision", 0, md.getScale(1));
        Assert.assertEquals("uint8 precision", 0, md.getScale(2));
        Assert.assertEquals("int16 precision", 0, md.getScale(3));
        Assert.assertEquals("uint16 precision", 0, md.getScale(4));
        Assert.assertEquals("int24 precision", 0, md.getScale(5));
        Assert.assertEquals("uint24 precision", 0, md.getScale(6));
        Assert.assertEquals("int32 precision", 0, md.getScale(7));
        Assert.assertEquals("uint32 precision", 0, md.getScale(8));
        Assert.assertEquals("int64 precision", 0, md.getScale(9));
        Assert.assertEquals("uint64 precision", 0, md.getScale(10));
        Assert.assertEquals("float32 precision", 31, md.getScale(11));
        Assert.assertEquals("float64 precision", 31, md.getScale(12));
        Assert.assertEquals("timestamp precision", 0, md.getScale(13));
        Assert.assertEquals("date precision", 0, md.getScale(14));
        Assert.assertEquals("time precision", 0, md.getScale(15));
        Assert.assertEquals("datetime precision", 0, md.getScale(16));
        Assert.assertEquals("year precision",  0, md.getScale(17));
        Assert.assertEquals("decimal precision", 2, md.getScale(18));
        Assert.assertEquals("text precision", 0, md.getScale(19));
        Assert.assertEquals("blob precision", 0, md.getScale(20));
        Assert.assertEquals("varchar precision", 0, md.getScale(21));
        Assert.assertEquals("varbinary precision", 0, md.getScale(22));
        Assert.assertEquals("char precision", 0, md.getScale(23));
        Assert.assertEquals("binary precision", 0, md.getScale(24));
        Assert.assertEquals("bit precision", 0, md.getScale(25));
        Assert.assertEquals("enum precision", 0, md.getScale(26));
        Assert.assertEquals("set precision", 0, md.getScale(27));
        Assert.assertEquals("tuple precision", 0, md.getScale(28));
        Assert.assertEquals("varbinary precision", 0, md.getScale(29));

        conn.setIncludedFields(Query.ExecuteOptions.IncludedFields.TYPE_AND_NAME);
        conn.setIncludedFields(Query.ExecuteOptions.IncludedFields.TYPE_AND_NAME);
        for (int i = 0; i < fieldList.size(); i++) {
            Assert.assertEquals(fieldList.get(i).getName() + " - getScale is 0 for all when lack of included fields", 0, md.getScale(i + 1));
        }
    }

    @Test public void testGetColumnClassName() throws SQLException {
        VitessConnection conn = getVitessConnection();
        List<FieldWithMetadata> fieldList = getFieldList(conn);
        VitessResultSetMetaData md = new VitessResultSetMetaData(fieldList);
        Assert.assertEquals("java.lang.Integer", md.getColumnClassName(1));
        Assert.assertEquals("java.lang.Integer", md.getColumnClassName(2));
        Assert.assertEquals("java.lang.Integer", md.getColumnClassName(3));
        Assert.assertEquals("java.lang.Integer", md.getColumnClassName(4));
        Assert.assertEquals("java.lang.Integer", md.getColumnClassName(5));
        Assert.assertEquals("java.lang.Integer", md.getColumnClassName(6));
        Assert.assertEquals("java.lang.Integer", md.getColumnClassName(7));
        Assert.assertEquals("java.lang.Long", md.getColumnClassName(8));
        Assert.assertEquals("java.lang.Long", md.getColumnClassName(9));
        Assert.assertEquals("java.math.BigInteger", md.getColumnClassName(10));
        Assert.assertEquals("java.lang.Double", md.getColumnClassName(11));
        Assert.assertEquals("java.lang.Double", md.getColumnClassName(12));
        Assert.assertEquals("java.sql.Timestamp", md.getColumnClassName(13));
        Assert.assertEquals("java.sql.Date", md.getColumnClassName(14));
        Assert.assertEquals("java.sql.Time", md.getColumnClassName(15));
        Assert.assertEquals("java.sql.Timestamp", md.getColumnClassName(16));
        Assert.assertEquals("java.sql.Date", md.getColumnClassName(17));
        Assert.assertEquals("java.math.BigDecimal", md.getColumnClassName(18));
        Assert.assertEquals("java.lang.String", md.getColumnClassName(19));
        Assert.assertEquals("java.lang.Object", md.getColumnClassName(20));
        Assert.assertEquals("java.lang.String", md.getColumnClassName(21));
        Assert.assertEquals("[B", md.getColumnClassName(22));
        Assert.assertEquals("java.lang.String", md.getColumnClassName(23));
        Assert.assertEquals("[B", md.getColumnClassName(24));
        Assert.assertEquals("java.lang.Boolean", md.getColumnClassName(25));
        Assert.assertEquals("java.lang.String", md.getColumnClassName(26));
        Assert.assertEquals("java.lang.String", md.getColumnClassName(27));
        Assert.assertEquals("java.lang.Object", md.getColumnClassName(28));
        Assert.assertEquals("[B", md.getColumnClassName(29));

        conn.setYearIsDateType(false);
        md = new VitessResultSetMetaData(fieldList);
        Assert.assertEquals("java.lang.Short", md.getColumnClassName(17));

        conn.setIncludedFields(Query.ExecuteOptions.IncludedFields.TYPE_AND_NAME);
        for (int i = 0; i < fieldList.size(); i++) {
            Assert.assertEquals(fieldList.get(i).getName() + " - class name is null when no included fields", null, md.getColumnClassName(i + 1));
        }
    }

    /**
     * Some of the tests above verify that their particular part honors the IncludedFields.ALL value, but
     * this further verifies that when someone has disabled ALL, the values returned by the driver are basically the same
     * as what they used to be before we supported returning all fields.
     */
    @Test public void testDefaultValuesWithoutIncludedFields() throws SQLException {
        VitessConnection conn = getVitessConnection();
        conn.setIncludedFields(Query.ExecuteOptions.IncludedFields.TYPE_AND_NAME);
        List<FieldWithMetadata> fields = getFieldList(conn);
        VitessResultSetMetaData vitessResultSetMetaData = new VitessResultSetMetaData(fields);
        for (int i = 1; i < fields.size() + 1; i++) {
            FieldWithMetadata field = fields.get(i - 1);
            Assert.assertEquals(false, vitessResultSetMetaData.isAutoIncrement(i));
            boolean shouldBeSensitive = true;
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
                    shouldBeSensitive = false;
                    break;
            }
            Assert.assertEquals(shouldBeSensitive, vitessResultSetMetaData.isCaseSensitive(i));
            Assert.assertEquals(field.getName(), true, vitessResultSetMetaData.isSearchable(i));
            Assert.assertEquals(field.getName(), false, vitessResultSetMetaData.isCurrency(i));
            Assert.assertEquals(field.getName(), ResultSetMetaData.columnNullableUnknown, vitessResultSetMetaData.isNullable(i));
            Assert.assertEquals(field.getName(), false, vitessResultSetMetaData.isSigned(i));
            Assert.assertEquals(field.getName(), 0, vitessResultSetMetaData.getColumnDisplaySize(i));
            Assert.assertEquals(field.getName(), field.getName(), vitessResultSetMetaData.getColumnLabel(i));
            Assert.assertEquals(field.getName(), field.getName(), vitessResultSetMetaData.getColumnName(i));
            Assert.assertEquals(field.getName(), 0, vitessResultSetMetaData.getPrecision(i));
            Assert.assertEquals(field.getName(), 0, vitessResultSetMetaData.getScale(i));
            Assert.assertEquals(field.getName(), null, vitessResultSetMetaData.getTableName(i));
            Assert.assertEquals(field.getName(), null, vitessResultSetMetaData.getCatalogName(i));
            // These two do not depend on IncludedFields and are covered by tests above
            //Assert.assertEquals(field.getName(), null, vitessResultSetMetaData.getColumnType(i));
            //Assert.assertEquals(field.getName(), null, vitessResultSetMetaData.getColumnTypeName(i));
            Assert.assertEquals(field.getName(), false, vitessResultSetMetaData.isReadOnly(i));
            Assert.assertEquals(field.getName(), true, vitessResultSetMetaData.isWritable(i));
            Assert.assertEquals(field.getName(), true, vitessResultSetMetaData.isDefinitelyWritable(i));
            Assert.assertEquals(field.getName(), null, vitessResultSetMetaData.getColumnClassName(i));
        }
    }
}

