package com.flipkart.vitess.jdbc.test;

import com.flipkart.vitess.jdbc.VitessDatabaseMetaData;
import com.flipkart.vitess.jdbc.VitessMySQLDatabaseMetadata;
import org.junit.Assert;
import org.junit.Test;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;

/**
 * Created by ashudeep.sharma on 08/03/16.
 */
public class VitessDatabaseMetadataTest {

    private ResultSet resultSet;

    @Test public void getPseudoColumnsTest() throws SQLException {
        VitessDatabaseMetaData vitessDatabaseMetaData = new VitessMySQLDatabaseMetadata(null);
        this.resultSet = vitessDatabaseMetaData.getPseudoColumns(null, null, null, null);

        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        Assert.assertEquals(resultSetMetaData.getColumnName(1), "TABLE_CAT");
        Assert.assertEquals(resultSetMetaData.getColumnName(2), "TABLE_SCHEM");
        Assert.assertEquals(resultSetMetaData.getColumnName(3), "TABLE_NAME");
        Assert.assertEquals(resultSetMetaData.getColumnName(4), "COLUMN_NAME");
        Assert.assertEquals(resultSetMetaData.getColumnName(5), "DATA_TYPE");
        Assert.assertEquals(resultSetMetaData.getColumnName(6), "COLUMN_SIZE");
        Assert.assertEquals(resultSetMetaData.getColumnName(7), "DECIMAL_DIGITS");
        Assert.assertEquals(resultSetMetaData.getColumnName(8), "NUM_PREC_RADIX");
        Assert.assertEquals(resultSetMetaData.getColumnName(9), "COLUMN_USAGE");
        Assert.assertEquals(resultSetMetaData.getColumnName(10), "REMARKS");
        Assert.assertEquals(resultSetMetaData.getColumnName(11), "CHAR_OCTET_LENGTH");
        Assert.assertEquals(resultSetMetaData.getColumnName(12), "IS_NULLABLE");
    }

    @Test public void getClientInfoPropertiesTest() throws SQLException {
        VitessDatabaseMetaData vitessDatabaseMetaData = new VitessMySQLDatabaseMetadata(null);
        this.resultSet = vitessDatabaseMetaData.getClientInfoProperties();

        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        Assert.assertEquals(resultSetMetaData.getColumnName(1), "NAME");
        Assert.assertEquals(resultSetMetaData.getColumnName(2), "MAX_LEN");
        Assert.assertEquals(resultSetMetaData.getColumnName(3), "DEFAULT_VALUE");
        Assert.assertEquals(resultSetMetaData.getColumnName(4), "DESCRIPTION");
    }

    @Test public void getSchemasTest() throws SQLException {
        VitessDatabaseMetaData vitessDatabaseMetaData = new VitessMySQLDatabaseMetadata(null);
        this.resultSet = vitessDatabaseMetaData.getSchemas(null, null);

        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        Assert.assertEquals(resultSetMetaData.getColumnName(1), "TABLE_CAT");
        Assert.assertEquals(resultSetMetaData.getColumnName(2), "TABLE_CATALOG");
    }

    @Test public void getAttributesTest() throws SQLException {
        VitessDatabaseMetaData vitessDatabaseMetaData = new VitessMySQLDatabaseMetadata(null);
        this.resultSet = vitessDatabaseMetaData.getAttributes(null, null, null, null);

        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        Assert.assertEquals(resultSetMetaData.getColumnName(1), "TYPE_CAT");
        Assert.assertEquals(resultSetMetaData.getColumnName(2), "TYPE_SCHEM");
        Assert.assertEquals(resultSetMetaData.getColumnName(3), "TYPE_NAME");
        Assert.assertEquals(resultSetMetaData.getColumnName(4), "ATTR_NAME");
        Assert.assertEquals(resultSetMetaData.getColumnName(5), "DATA_TYPE");
        Assert.assertEquals(resultSetMetaData.getColumnName(6), "ATTR_TYPE_NAME");
        Assert.assertEquals(resultSetMetaData.getColumnName(7), "ATTR_SIZE");
        Assert.assertEquals(resultSetMetaData.getColumnName(8), "DECIMAL_DIGITS");
        Assert.assertEquals(resultSetMetaData.getColumnName(9), "NUM_PREC_RADIX");
        Assert.assertEquals(resultSetMetaData.getColumnName(10), "NULLABLE");
        Assert.assertEquals(resultSetMetaData.getColumnName(11), "REMARKS");
        Assert.assertEquals(resultSetMetaData.getColumnName(12), "ATTR_DEF");
        Assert.assertEquals(resultSetMetaData.getColumnName(13), "SQL_DATA_TYPE");
        Assert.assertEquals(resultSetMetaData.getColumnName(14), "SQL_DATETIME_SUB");
        Assert.assertEquals(resultSetMetaData.getColumnName(15), "CHAR_OCTET_LENGTH");
        Assert.assertEquals(resultSetMetaData.getColumnName(16), "ORDINAL_POSITION");
        Assert.assertEquals(resultSetMetaData.getColumnName(17), "ISNULLABLE");
        Assert.assertEquals(resultSetMetaData.getColumnName(18), "SCOPE_CATALOG");
        Assert.assertEquals(resultSetMetaData.getColumnName(19), "SCOPE_SCHEMA");
        Assert.assertEquals(resultSetMetaData.getColumnName(20), "SCOPE_TABLE");
        Assert.assertEquals(resultSetMetaData.getColumnName(21), "SOURCE_DATA_TYPE");
    }

    @Test public void getSuperTablesTest() throws SQLException {

        VitessDatabaseMetaData vitessDatabaseMetaData = new VitessMySQLDatabaseMetadata(null);
        this.resultSet = vitessDatabaseMetaData.getSuperTables(null, null, null);

        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        Assert.assertEquals(resultSetMetaData.getColumnName(1), "TABLE_CAT");
        Assert.assertEquals(resultSetMetaData.getColumnName(2), "TYPE_SCHEM");
        Assert.assertEquals(resultSetMetaData.getColumnName(3), "TABLE_NAME");
        Assert.assertEquals(resultSetMetaData.getColumnName(4), "SUPERTABLE_NAME");
    }

    @Test public void getSuperTypesTest() throws SQLException {
        VitessDatabaseMetaData vitessDatabaseMetaData = new VitessMySQLDatabaseMetadata(null);
        this.resultSet = vitessDatabaseMetaData.getSuperTypes(null, null, null);

        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        Assert.assertEquals(resultSetMetaData.getColumnName(1), "TYPE_CAT");
        Assert.assertEquals(resultSetMetaData.getColumnName(2), "TYPE_SCHEM");
        Assert.assertEquals(resultSetMetaData.getColumnName(3), "TYPE_NAME");
        Assert.assertEquals(resultSetMetaData.getColumnName(4), "SUPERTYPE_CAT");
        Assert.assertEquals(resultSetMetaData.getColumnName(5), "SUPERTYPE_SCHEM");
        Assert.assertEquals(resultSetMetaData.getColumnName(6), "SUPERTYPE_NAME");
    }

    @Test public void getUDTsTest() throws SQLException {
        VitessDatabaseMetaData vitessDatabaseMetaData = new VitessMySQLDatabaseMetadata(null);
        this.resultSet = vitessDatabaseMetaData.getUDTs(null, null, null, null);

        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        Assert.assertEquals(resultSetMetaData.getColumnName(1), "TYPE_CAT");
        Assert.assertEquals(resultSetMetaData.getColumnName(2), "TYPE_SCHEM");
        Assert.assertEquals(resultSetMetaData.getColumnName(3), "TYPE_NAME");
        Assert.assertEquals(resultSetMetaData.getColumnName(4), "CLASS_NAME");
        Assert.assertEquals(resultSetMetaData.getColumnName(5), "DATA_TYPE");
        Assert.assertEquals(resultSetMetaData.getColumnName(6), "REMARKS");
        Assert.assertEquals(resultSetMetaData.getColumnName(7), "BASE_TYPE");
    }

    @Test public void getTypeInfoTest() throws SQLException {
        VitessDatabaseMetaData vitessDatabaseMetaData = new VitessMySQLDatabaseMetadata(null);
        this.resultSet = vitessDatabaseMetaData.getTypeInfo();

        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        Assert.assertEquals(resultSetMetaData.getColumnName(1), "TYPE_NAME");
        Assert.assertEquals(resultSetMetaData.getColumnName(2), "DATA_TYPE");
        Assert.assertEquals(resultSetMetaData.getColumnName(3), "PRECISION");
        Assert.assertEquals(resultSetMetaData.getColumnName(4), "LITERAL_PREFIX");
        Assert.assertEquals(resultSetMetaData.getColumnName(5), "LITERAL_SUFFIX");
        Assert.assertEquals(resultSetMetaData.getColumnName(6), "CREATE_PARAMS");
        Assert.assertEquals(resultSetMetaData.getColumnName(7), "NULLABLE");
        Assert.assertEquals(resultSetMetaData.getColumnName(8), "CASE_SENSITIVE");
        Assert.assertEquals(resultSetMetaData.getColumnName(9), "SEARCHABLE");
        Assert.assertEquals(resultSetMetaData.getColumnName(10), "UNSIGNED_ATTRIBUTE");
        Assert.assertEquals(resultSetMetaData.getColumnName(11), "FIXED_PREC_SCALE");
        Assert.assertEquals(resultSetMetaData.getColumnName(12), "AUTO_INCREMENT");
        Assert.assertEquals(resultSetMetaData.getColumnName(13), "LOCAL_TYPE_NAME");
        Assert.assertEquals(resultSetMetaData.getColumnName(14), "MINIMUM_SCALE");
        Assert.assertEquals(resultSetMetaData.getColumnName(15), "MAXIMUM_SCALE");
        Assert.assertEquals(resultSetMetaData.getColumnName(16), "SQL_DATA_TYPE");
        Assert.assertEquals(resultSetMetaData.getColumnName(17), "SQL_DATETIME_SUB");
        Assert.assertEquals(resultSetMetaData.getColumnName(18), "NUM_PREC_RADIX");

        //Check for ResultSet Data as well
    }

    @Test public void getTableTypesTest() throws SQLException {

        VitessDatabaseMetaData vitessDatabaseMetaData = new VitessMySQLDatabaseMetadata(null);
        this.resultSet = vitessDatabaseMetaData.getTableTypes();

        ArrayList<String> data = new ArrayList<String>();
        while (resultSet.next()) {
            data.add(resultSet.getString("TABLE_TYPE"));
        }

        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        Assert.assertEquals(resultSetMetaData.getColumnName(1), "table_type");
        //Checking Data
        Assert.assertEquals(data.get(0), "LOCAL TEMPORARY");
        Assert.assertEquals(data.get(1), "SYSTEM TABLES");
        Assert.assertEquals(data.get(2), "SYSTEM VIEW");
        Assert.assertEquals(data.get(3), "TABLE");
        Assert.assertEquals(data.get(4), "VIEW");
    }

    @Test public void getSchemasTest2() throws SQLException {
        VitessDatabaseMetaData vitessDatabaseMetaData = new VitessMySQLDatabaseMetadata(null);
        this.resultSet = vitessDatabaseMetaData.getSchemas();

        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        Assert.assertEquals(resultSetMetaData.getColumnName(1), "TABLE_SCHEM");
        Assert.assertEquals(resultSetMetaData.getColumnName(2), "TABLE_CATALOG");
    }

}
