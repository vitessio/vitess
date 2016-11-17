package com.flipkart.vitess.jdbc.test;

import com.flipkart.vitess.jdbc.VitessResultSetMetaData;
import com.flipkart.vitess.util.Constants;
import com.youtube.vitess.proto.Query;
import org.junit.Assert;
import org.junit.Test;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by ashudeep.sharma on 08/02/16.
 */
public class VitessResultSetMetadataTest {

    private List<Query.Field> fieldList;

    public void initializeFieldList() {

        fieldList = new ArrayList<>();
        fieldList.add(Query.Field.newBuilder().setName("col1").setType(Query.Type.INT8).build());
        fieldList.add(Query.Field.newBuilder().setName("col2").setType(Query.Type.UINT8).build());
        fieldList.add(Query.Field.newBuilder().setName("col3").setType(Query.Type.INT16).build());
        fieldList.add(Query.Field.newBuilder().setName("col4").setType(Query.Type.UINT16).build());
        fieldList.add(Query.Field.newBuilder().setName("col5").setType(Query.Type.INT24).build());
        fieldList.add(Query.Field.newBuilder().setName("col6").setType(Query.Type.UINT24).build());
        fieldList.add(Query.Field.newBuilder().setName("col7").setType(Query.Type.INT32).build());
        fieldList.add(Query.Field.newBuilder().setName("col8").setType(Query.Type.UINT32).build());
        fieldList.add(Query.Field.newBuilder().setName("col9").setType(Query.Type.INT64).build());
        fieldList.add(Query.Field.newBuilder().setName("col10").setType(Query.Type.UINT64).build());
        fieldList
            .add(Query.Field.newBuilder().setName("col11").setType(Query.Type.FLOAT32).build());
        fieldList
            .add(Query.Field.newBuilder().setName("col12").setType(Query.Type.FLOAT64).build());
        fieldList
            .add(Query.Field.newBuilder().setName("col13").setType(Query.Type.TIMESTAMP).build());
        fieldList.add(Query.Field.newBuilder().setName("col14").setType(Query.Type.DATE).build());
        fieldList.add(Query.Field.newBuilder().setName("col15").setType(Query.Type.TIME).build());
        fieldList
            .add(Query.Field.newBuilder().setName("col16").setType(Query.Type.DATETIME).build());
        fieldList.add(Query.Field.newBuilder().setName("col17").setType(Query.Type.YEAR).build());
        fieldList
            .add(Query.Field.newBuilder().setName("col18").setType(Query.Type.DECIMAL).build());
        fieldList.add(Query.Field.newBuilder().setName("col19").setType(Query.Type.TEXT).build());
        fieldList.add(Query.Field.newBuilder().setName("col20").setType(Query.Type.BLOB).build());
        fieldList
            .add(Query.Field.newBuilder().setName("col21").setType(Query.Type.VARCHAR).build());
        fieldList
            .add(Query.Field.newBuilder().setName("col22").setType(Query.Type.VARBINARY).build());
        fieldList.add(Query.Field.newBuilder().setName("col23").setType(Query.Type.CHAR).build());
        fieldList.add(Query.Field.newBuilder().setName("col24").setType(Query.Type.BINARY).build());
        fieldList.add(Query.Field.newBuilder().setName("col25").setType(Query.Type.BIT).build());
        fieldList.add(Query.Field.newBuilder().setName("col26").setType(Query.Type.ENUM).build());
        fieldList.add(Query.Field.newBuilder().setName("col27").setType(Query.Type.SET).build());
        fieldList.add(Query.Field.newBuilder().setName("col28").setType(Query.Type.TUPLE).build());
    }

    public List<Query.Field> getFieldList() {

        initializeFieldList();
        return this.fieldList;
    }

    @Test public void testgetColumnCount() throws SQLException {

        List<Query.Field> fieldList = getFieldList();
        VitessResultSetMetaData vitessResultSetMetadata = new VitessResultSetMetaData(fieldList);
        Assert.assertEquals(28, vitessResultSetMetadata.getColumnCount());
    }

    @Test public void testgetColumnName() throws SQLException {

        List<Query.Field> fieldList = getFieldList();
        VitessResultSetMetaData vitessResultSetMetadata = new VitessResultSetMetaData(fieldList);
        for (int i = 1; i <= vitessResultSetMetadata.getColumnCount(); i++) {
            Assert.assertEquals("col" + i, vitessResultSetMetadata.getColumnName(i));
        }
    }

    @Test public void testgetColumnTypeName() throws SQLException {

        List<Query.Field> fieldList = getFieldList();
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

    @Test public void testgetColumnType() throws SQLException {

        List<Query.Field> fieldList = getFieldList();
        VitessResultSetMetaData vitessResultSetMetadata = new VitessResultSetMetaData(fieldList);
        Assert.assertEquals(-6, vitessResultSetMetadata.getColumnType(1));
        Assert.assertEquals(-6, vitessResultSetMetadata.getColumnType(2));
        Assert.assertEquals(5, vitessResultSetMetadata.getColumnType(3));
        Assert.assertEquals(5, vitessResultSetMetadata.getColumnType(4));
        Assert.assertEquals(4, vitessResultSetMetadata.getColumnType(5));
        Assert.assertEquals(4, vitessResultSetMetadata.getColumnType(6));
        Assert.assertEquals(4, vitessResultSetMetadata.getColumnType(7));
        Assert.assertEquals(4, vitessResultSetMetadata.getColumnType(8));
        Assert.assertEquals(-5, vitessResultSetMetadata.getColumnType(9));
        Assert.assertEquals(-5, vitessResultSetMetadata.getColumnType(10));
        Assert.assertEquals(6, vitessResultSetMetadata.getColumnType(11));
        Assert.assertEquals(8, vitessResultSetMetadata.getColumnType(12));
        Assert.assertEquals(93, vitessResultSetMetadata.getColumnType(13));
        Assert.assertEquals(91, vitessResultSetMetadata.getColumnType(14));
        Assert.assertEquals(92, vitessResultSetMetadata.getColumnType(15));
        Assert.assertEquals(93, vitessResultSetMetadata.getColumnType(16));
        Assert.assertEquals(5, vitessResultSetMetadata.getColumnType(17));
        Assert.assertEquals(3, vitessResultSetMetadata.getColumnType(18));
        Assert.assertEquals(12, vitessResultSetMetadata.getColumnType(19));
        Assert.assertEquals(2004, vitessResultSetMetadata.getColumnType(20));
        Assert.assertEquals(12, vitessResultSetMetadata.getColumnType(21));
        Assert.assertEquals(-3, vitessResultSetMetadata.getColumnType(22));
        Assert.assertEquals(1, vitessResultSetMetadata.getColumnType(23));
        Assert.assertEquals(-2, vitessResultSetMetadata.getColumnType(24));
        Assert.assertEquals(-7, vitessResultSetMetadata.getColumnType(25));
        Assert.assertEquals(1, vitessResultSetMetadata.getColumnType(26));
        Assert.assertEquals(1, vitessResultSetMetadata.getColumnType(27));
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
        List<Query.Field> fieldList = getFieldList();
        VitessResultSetMetaData vitessResultSetMetaData = new VitessResultSetMetaData(fieldList);
        for (int i = 1; i <= vitessResultSetMetaData.getColumnCount(); i++) {
            Assert.assertEquals("col" + i, vitessResultSetMetaData.getColumnLabel(i));
        }
    }

    @Test public void isReadOnlyTest() throws SQLException {
        List<Query.Field> fieldList = getFieldList();
        VitessResultSetMetaData vitessResultSetMetadata = new VitessResultSetMetaData(fieldList);
        for (int i = 1; i <= vitessResultSetMetadata.getColumnCount(); i++) {
            Assert.assertEquals(vitessResultSetMetadata.isReadOnly(i), false);
            Assert.assertEquals(vitessResultSetMetadata.isWritable(i), true);
            Assert.assertEquals(vitessResultSetMetadata.isDefinitelyWritable(i), true);
        }
    }

    @Test public void getColumnClassNameTest() throws SQLException {
        List<Query.Field> fieldList = getFieldList();
        VitessResultSetMetaData vitessResultSetMetadata = new VitessResultSetMetaData(fieldList);
        for (int i = 1; i <= vitessResultSetMetadata.getColumnCount(); i++) {
            Assert.assertEquals(vitessResultSetMetadata.getColumnClassName(i), null);
        }
    }

    @Test public void getSchemaNameTest() throws SQLException {
        List<Query.Field> fieldList = getFieldList();
        VitessResultSetMetaData vitessResultSetMetaData = new VitessResultSetMetaData(fieldList);
        Assert.assertEquals(vitessResultSetMetaData.getSchemaName(1), null);
        Assert.assertEquals(vitessResultSetMetaData.getTableName(1), null);
        Assert.assertEquals(vitessResultSetMetaData.getCatalogName(1), null);
        Assert.assertEquals(vitessResultSetMetaData.getSchemaName(1), null);
        Assert.assertEquals(vitessResultSetMetaData.getPrecision(1), 0);
        Assert.assertEquals(vitessResultSetMetaData.getScale(1), 0);
        Assert.assertEquals(vitessResultSetMetaData.getColumnDisplaySize(1), 0);
        Assert.assertEquals(vitessResultSetMetaData.isCurrency(1), false);
    }
}

