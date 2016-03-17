package com.flipkart.vitess.jdbc.test;

import com.flipkart.vitess.jdbc.*;
import com.flipkart.vitess.util.Constants;
import com.google.protobuf.ByteString;
import com.youtube.vitess.client.cursor.Cursor;
import com.youtube.vitess.client.cursor.SimpleCursor;
import com.youtube.vitess.proto.Query;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.sql.*;
import java.util.ArrayList;

/**
 * Created by ashudeep.sharma on 08/03/16.
 */
@RunWith(PowerMockRunner.class) @PrepareForTest(VitessMySQLDatabaseMetadata.class) public class VitessDatabaseMetadataTest {

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
            data.add(resultSet.getString("table_type"));
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

    @Test public void allProceduresAreCallableTest() throws SQLException {
        VitessDatabaseMetaData vitessDatabaseMetaData = new VitessMySQLDatabaseMetadata(null);

        Assert.assertEquals(vitessDatabaseMetaData.allProceduresAreCallable(), false);
    }

    @Test public void allTablesAreSelectableTest() throws SQLException {
        VitessDatabaseMetaData vitessDatabaseMetaData = new VitessMySQLDatabaseMetadata(null);

        Assert.assertEquals(vitessDatabaseMetaData.allTablesAreSelectable(), false);
    }

    @Test public void nullsAreSortedHighTest() throws SQLException {
        VitessDatabaseMetaData vitessDatabaseMetaData = new VitessMySQLDatabaseMetadata(null);
        VitessDatabaseMetaData vitessMariaDBDatabaseMetadata =
            new VitessMariaDBDatabaseMetadata(null);

        Assert.assertEquals(vitessDatabaseMetaData.nullsAreSortedHigh(), false);
        Assert.assertEquals(vitessMariaDBDatabaseMetadata.nullsAreSortedHigh(), false);
    }

    @Test public void nullsAreSortedLowTest() throws SQLException {
        VitessDatabaseMetaData vitessDatabaseMetaData = new VitessMySQLDatabaseMetadata(null);
        VitessDatabaseMetaData vitessMariaDBDatabaseMetadata =
            new VitessMariaDBDatabaseMetadata(null);

        Assert.assertEquals(vitessDatabaseMetaData.nullsAreSortedLow(), true);
        Assert.assertEquals(vitessMariaDBDatabaseMetadata.nullsAreSortedLow(), true);
    }

    @Test public void nullsAreSortedAtStartTest() throws SQLException {
        VitessDatabaseMetaData vitessMySQLDatabaseMetaData = new VitessMySQLDatabaseMetadata(null);
        VitessDatabaseMetaData vitessMariaDBDatabaseMetadata =
            new VitessMariaDBDatabaseMetadata(null);

        Assert.assertEquals(vitessMySQLDatabaseMetaData.nullsAreSortedAtStart(), false);
        Assert.assertEquals(vitessMariaDBDatabaseMetadata.nullsAreSortedAtStart(), false);
    }

    @Test public void nullsAreSortedAtEndTest() throws SQLException {
        VitessDatabaseMetaData vitessMySQLDatabaseMetaData = new VitessMySQLDatabaseMetadata(null);
        VitessDatabaseMetaData vitessMariaDBDatabaseMetadata =
            new VitessMariaDBDatabaseMetadata(null);

        Assert.assertEquals(vitessMySQLDatabaseMetaData.nullsAreSortedAtEnd(), false);
        Assert.assertEquals(vitessMariaDBDatabaseMetadata.nullsAreSortedAtEnd(), true);
    }

    @Test public void getDatabaseProductNameTest() throws SQLException {
        VitessDatabaseMetaData vitessDatabaseMetaData = new VitessMySQLDatabaseMetadata(null);

        Assert.assertEquals(vitessDatabaseMetaData.getDatabaseProductName(), "MySQL");
    }

    @Test public void getDriverVersionTest() throws SQLException {
        VitessDatabaseMetaData vitessDatabaseMetaData = new VitessMySQLDatabaseMetadata(null);
        StringBuilder driverVersionBuilder = new StringBuilder();
        driverVersionBuilder.append(Constants.DRIVER_MAJOR_VERSION);
        driverVersionBuilder.append(".");
        driverVersionBuilder.append(Constants.DRIVER_MINOR_VERSION);
        Assert.assertEquals(vitessDatabaseMetaData.getDriverVersion(),
            driverVersionBuilder.toString());
    }

    @Test public void getDriverMajorVersionTest() throws SQLException {
        VitessDatabaseMetaData vitessDatabaseMetaData = new VitessMySQLDatabaseMetadata(null);

        Assert.assertEquals(vitessDatabaseMetaData.getDriverMajorVersion(),
            Constants.DRIVER_MAJOR_VERSION);
    }

    @Test public void getDriverMinorVersionTest() throws SQLException {
        VitessDatabaseMetaData vitessDatabaseMetaData = new VitessMySQLDatabaseMetadata(null);

        Assert.assertEquals(vitessDatabaseMetaData.getDriverMinorVersion(),
            Constants.DRIVER_MINOR_VERSION);
    }

    @Test public void getSearchStringEscapeTest() throws SQLException {
        VitessDatabaseMetaData vitessDatabaseMetaData = new VitessMySQLDatabaseMetadata(null);

        Assert.assertEquals(vitessDatabaseMetaData.getSearchStringEscape(), "\\");
    }

    @Test public void getExtraNameCharactersTest() throws SQLException {
        VitessDatabaseMetaData vitessDatabaseMetaData = new VitessMySQLDatabaseMetadata(null);

        Assert.assertEquals(vitessDatabaseMetaData.getExtraNameCharacters(), "#@");
    }

    @Test public void supportsAlterTableWithAddColumnTest() throws SQLException {
        VitessDatabaseMetaData vitessDatabaseMetaData = new VitessMySQLDatabaseMetadata(null);

        Assert.assertEquals(vitessDatabaseMetaData.supportsAlterTableWithAddColumn(), false);
    }

    @Test public void supportsAlterTableWithDropColumnTest() throws SQLException {
        VitessDatabaseMetaData vitessDatabaseMetaData = new VitessMySQLDatabaseMetadata(null);

        Assert.assertEquals(vitessDatabaseMetaData.supportsAlterTableWithDropColumn(), false);
    }

    @Test public void supportsColumnAliasingTest() throws SQLException {
        VitessDatabaseMetaData vitessDatabaseMetaData = new VitessMySQLDatabaseMetadata(null);

        Assert.assertEquals(vitessDatabaseMetaData.supportsColumnAliasing(), true);
    }

    @Test public void nullPlusNonNullIsNullTest() throws SQLException {
        VitessDatabaseMetaData vitessDatabaseMetaData = new VitessMySQLDatabaseMetadata(null);

        Assert.assertEquals(vitessDatabaseMetaData.nullPlusNonNullIsNull(), true);
    }

    @Test public void supportsExpressionsInOrderByTest() throws SQLException {
        VitessDatabaseMetaData vitessDatabaseMetaData = new VitessMySQLDatabaseMetadata(null);

        Assert.assertEquals(vitessDatabaseMetaData.supportsExpressionsInOrderBy(), false);
    }

    @Test public void supportsOrderByUnrelatedTest() throws SQLException {
        VitessDatabaseMetaData vitessDatabaseMetaData = new VitessMySQLDatabaseMetadata(null);

        Assert.assertEquals(vitessDatabaseMetaData.supportsOrderByUnrelated(), false);
    }

    @Test public void supportsGroupByTest() throws SQLException {
        VitessDatabaseMetaData vitessDatabaseMetaData = new VitessMySQLDatabaseMetadata(null);

        Assert.assertEquals(vitessDatabaseMetaData.supportsGroupBy(), false);
    }

    @Test public void supportsGroupByUnrelatedTest() throws SQLException {
        VitessDatabaseMetaData vitessDatabaseMetaData = new VitessMySQLDatabaseMetadata(null);

        Assert.assertEquals(vitessDatabaseMetaData.supportsGroupByUnrelated(), false);
    }

    @Test public void supportsGroupByBeyondSelectTest() throws SQLException {
        VitessDatabaseMetaData vitessDatabaseMetaData = new VitessMySQLDatabaseMetadata(null);

        Assert.assertEquals(vitessDatabaseMetaData.supportsGroupByBeyondSelect(), false);
    }

    @Test public void supportsLikeEscapeClauseTest() throws SQLException {
        VitessDatabaseMetaData vitessDatabaseMetaData = new VitessMySQLDatabaseMetadata(null);

        Assert.assertEquals(vitessDatabaseMetaData.supportsLikeEscapeClause(), true);
    }

    @Test public void supportsMultipleResultSetsTest() throws SQLException {
        VitessDatabaseMetaData vitessDatabaseMetaData = new VitessMySQLDatabaseMetadata(null);

        Assert.assertEquals(vitessDatabaseMetaData.supportsMultipleResultSets(), false);
    }

    @Test public void supportsMultipleTransactionsTest() throws SQLException {
        VitessDatabaseMetaData vitessDatabaseMetaData = new VitessMySQLDatabaseMetadata(null);

        Assert.assertEquals(vitessDatabaseMetaData.supportsMultipleTransactions(), true);
    }

    @Test public void supportsNonNullableColumnsTest() throws SQLException {
        VitessDatabaseMetaData vitessDatabaseMetaData = new VitessMySQLDatabaseMetadata(null);

        Assert.assertEquals(vitessDatabaseMetaData.supportsNonNullableColumns(), true);
    }

    @Test public void supportsMinimumSQLGrammarTest() throws SQLException {
        VitessDatabaseMetaData vitessDatabaseMetaData = new VitessMySQLDatabaseMetadata(null);

        Assert.assertEquals(vitessDatabaseMetaData.supportsMinimumSQLGrammar(), true);
    }

    @Test public void supportsCoreSQLGrammarTest() throws SQLException {
        VitessDatabaseMetaData vitessDatabaseMetaData = new VitessMySQLDatabaseMetadata(null);

        Assert.assertEquals(vitessDatabaseMetaData.supportsCoreSQLGrammar(), false);
    }

    @Test public void supportsExtendedSQLGrammarTest() throws SQLException {
        VitessDatabaseMetaData vitessDatabaseMetaData = new VitessMySQLDatabaseMetadata(null);

        Assert.assertEquals(vitessDatabaseMetaData.supportsExtendedSQLGrammar(), false);
    }

    @Test public void supportsOuterJoinsTest() throws SQLException {
        VitessDatabaseMetaData vitessDatabaseMetaData = new VitessMySQLDatabaseMetadata(null);

        Assert.assertEquals(vitessDatabaseMetaData.supportsOuterJoins(), false);
    }

    @Test public void supportsFullOuterJoinsTest() throws SQLException {
        VitessDatabaseMetaData vitessDatabaseMetaData = new VitessMySQLDatabaseMetadata(null);

        Assert.assertEquals(vitessDatabaseMetaData.supportsFullOuterJoins(), false);
    }

    @Test public void supportsLimitedOuterJoinsTest() throws SQLException {
        VitessDatabaseMetaData vitessDatabaseMetaData = new VitessMySQLDatabaseMetadata(null);

        Assert.assertEquals(vitessDatabaseMetaData.supportsLimitedOuterJoins(), false);
    }

    @Test public void getSchemaTermTest() throws SQLException {
        VitessDatabaseMetaData vitessDatabaseMetaData = new VitessMySQLDatabaseMetadata(null);

        Assert.assertEquals(vitessDatabaseMetaData.getSchemaTerm(), "");
    }

    @Test public void getProcedureTermTest() throws SQLException {
        VitessDatabaseMetaData vitessDatabaseMetaData = new VitessMySQLDatabaseMetadata(null);

        Assert.assertEquals(vitessDatabaseMetaData.getProcedureTerm(), "procedure");
    }

    @Test public void getCatalogTermTest() throws SQLException {
        VitessDatabaseMetaData vitessDatabaseMetaData = new VitessMySQLDatabaseMetadata(null);

        Assert.assertEquals(vitessDatabaseMetaData.getCatalogTerm(), "database");
    }

    @Test public void isCatalogAtStartTest() throws SQLException {
        VitessDatabaseMetaData vitessDatabaseMetaData = new VitessMySQLDatabaseMetadata(null);

        Assert.assertEquals(vitessDatabaseMetaData.isCatalogAtStart(), true);
    }

    @Test public void getCatalogSeparatorTest() throws SQLException {
        VitessDatabaseMetaData vitessDatabaseMetaData = new VitessMySQLDatabaseMetadata(null);

        Assert.assertEquals(vitessDatabaseMetaData.getCatalogSeparator(), ".");
    }

    @Test public void supportsSchemasInDataManipulationTest() throws SQLException {
        VitessDatabaseMetaData vitessDatabaseMetaData = new VitessMySQLDatabaseMetadata(null);

        Assert.assertEquals(vitessDatabaseMetaData.supportsSchemasInDataManipulation(), false);
    }

    @Test public void supportsSchemasInProcedureCallsTest() throws SQLException {
        VitessDatabaseMetaData vitessDatabaseMetaData = new VitessMySQLDatabaseMetadata(null);

        Assert.assertEquals(vitessDatabaseMetaData.supportsSchemasInProcedureCalls(), false);
    }

    @Test public void supportsSchemasInTableDefinitionsTest() throws SQLException {
        VitessDatabaseMetaData vitessDatabaseMetaData = new VitessMySQLDatabaseMetadata(null);

        Assert.assertEquals(vitessDatabaseMetaData.supportsSchemasInTableDefinitions(), false);
    }

    @Test public void supportsSchemasInIndexDefinitionsTest() throws SQLException {
        VitessDatabaseMetaData vitessDatabaseMetaData = new VitessMySQLDatabaseMetadata(null);

        Assert.assertEquals(vitessDatabaseMetaData.supportsSchemasInIndexDefinitions(), false);
    }

    @Test public void supportsSelectForUpdateTest() throws SQLException {
        VitessDatabaseMetaData vitessDatabaseMetaData = new VitessMySQLDatabaseMetadata(null);

        Assert.assertEquals(vitessDatabaseMetaData.supportsSelectForUpdate(), false);
    }

    @Test public void supportsStoredProceduresTest() throws SQLException {
        VitessDatabaseMetaData vitessDatabaseMetaData = new VitessMySQLDatabaseMetadata(null);

        Assert.assertEquals(vitessDatabaseMetaData.supportsStoredProcedures(), false);
    }

    @Test public void supportsSubqueriesInComparisonsTest() throws SQLException {
        VitessDatabaseMetaData vitessDatabaseMetaData = new VitessMySQLDatabaseMetadata(null);

        Assert.assertEquals(vitessDatabaseMetaData.supportsSubqueriesInComparisons(), false);
    }

    @Test public void supportsSubqueriesInExistsTest() throws SQLException {
        VitessDatabaseMetaData vitessDatabaseMetaData = new VitessMySQLDatabaseMetadata(null);

        Assert.assertEquals(vitessDatabaseMetaData.supportsSubqueriesInExists(), false);
    }

    @Test public void supportsSubqueriesInInsTest() throws SQLException {
        VitessDatabaseMetaData vitessDatabaseMetaData = new VitessMySQLDatabaseMetadata(null);

        Assert.assertEquals(vitessDatabaseMetaData.supportsSubqueriesInIns(), false);
    }

    @Test public void supportsSubqueriesInQuantifiedsTest() throws SQLException {
        VitessDatabaseMetaData vitessDatabaseMetaData = new VitessMySQLDatabaseMetadata(null);

        Assert.assertEquals(vitessDatabaseMetaData.supportsSubqueriesInQuantifieds(), false);
    }

    @Test public void supportsCorrelatedSubqueriesTest() throws SQLException {
        VitessDatabaseMetaData vitessDatabaseMetaData = new VitessMySQLDatabaseMetadata(null);

        Assert.assertEquals(vitessDatabaseMetaData.supportsCorrelatedSubqueries(), false);
    }

    @Test public void supportsUnionTest() throws SQLException {
        VitessDatabaseMetaData vitessDatabaseMetaData = new VitessMySQLDatabaseMetadata(null);

        Assert.assertEquals(vitessDatabaseMetaData.supportsUnion(), false);
    }

    @Test public void supportsUnionAllTest() throws SQLException {
        VitessDatabaseMetaData vitessDatabaseMetaData = new VitessMySQLDatabaseMetadata(null);

        Assert.assertEquals(vitessDatabaseMetaData.supportsUnionAll(), false);
    }

    @Test public void supportsOpenCursorsAcrossRollbackTest() throws SQLException {
        VitessDatabaseMetaData vitessDatabaseMetaData = new VitessMySQLDatabaseMetadata(null);

        Assert.assertEquals(vitessDatabaseMetaData.supportsOpenCursorsAcrossRollback(), false);
    }

    @Test public void supportsOpenStatementsAcrossCommitTest() throws SQLException {
        VitessDatabaseMetaData vitessDatabaseMetaData = new VitessMySQLDatabaseMetadata(null);

        Assert.assertEquals(vitessDatabaseMetaData.supportsOpenStatementsAcrossCommit(), false);
    }

    @Test public void supportsOpenStatementsAcrossRollbackTest() throws SQLException {
        VitessDatabaseMetaData vitessDatabaseMetaData = new VitessMySQLDatabaseMetadata(null);

        Assert.assertEquals(vitessDatabaseMetaData.supportsOpenStatementsAcrossRollback(), false);
    }

    @Test public void supportsOpenCursorsAcrossCommitTest() throws SQLException {
        VitessDatabaseMetaData vitessDatabaseMetaData = new VitessMySQLDatabaseMetadata(null);

        Assert.assertEquals(vitessDatabaseMetaData.supportsOpenCursorsAcrossCommit(), false);
    }

    @Test public void getMaxBinaryLiteralLengthTest() throws SQLException {
        VitessDatabaseMetaData vitessDatabaseMetaData = new VitessMySQLDatabaseMetadata(null);

        Assert.assertEquals(vitessDatabaseMetaData.getMaxBinaryLiteralLength(), 16777208);
    }

    @Test public void getMaxCharLiteralLengthTest() throws SQLException {
        VitessDatabaseMetaData vitessDatabaseMetaData = new VitessMySQLDatabaseMetadata(null);

        Assert.assertEquals(vitessDatabaseMetaData.getMaxCharLiteralLength(), 16777208);
    }

    @Test public void getMaxColumnNameLengthTest() throws SQLException {
        VitessDatabaseMetaData vitessDatabaseMetaData = new VitessMySQLDatabaseMetadata(null);

        Assert.assertEquals(vitessDatabaseMetaData.getMaxColumnNameLength(), 64);
    }

    @Test public void getMaxColumnsInGroupByTest() throws SQLException {
        VitessDatabaseMetaData vitessDatabaseMetaData = new VitessMySQLDatabaseMetadata(null);

        Assert.assertEquals(vitessDatabaseMetaData.getMaxColumnsInGroupBy(), 64);
    }

    @Test public void getMaxColumnsInIndexTest() throws SQLException {
        VitessDatabaseMetaData vitessDatabaseMetaData = new VitessMySQLDatabaseMetadata(null);

        Assert.assertEquals(vitessDatabaseMetaData.getMaxColumnsInIndex(), 16);
    }

    @Test public void getMaxColumnsInOrderByTest() throws SQLException {
        VitessDatabaseMetaData vitessDatabaseMetaData = new VitessMySQLDatabaseMetadata(null);

        Assert.assertEquals(vitessDatabaseMetaData.getMaxColumnsInOrderBy(), 64);
    }

    @Test public void getMaxColumnsInSelectTest() throws SQLException {
        VitessDatabaseMetaData vitessDatabaseMetaData = new VitessMySQLDatabaseMetadata(null);

        Assert.assertEquals(vitessDatabaseMetaData.getMaxColumnsInSelect(), 256);
    }

    @Test public void getMaxIndexLengthTest() throws SQLException {
        VitessDatabaseMetaData vitessDatabaseMetaData = new VitessMySQLDatabaseMetadata(null);

        Assert.assertEquals(vitessDatabaseMetaData.getMaxIndexLength(), 256);
    }

    @Test public void doesMaxRowSizeIncludeBlobsTest() throws SQLException {
        VitessDatabaseMetaData vitessDatabaseMetaData = new VitessMySQLDatabaseMetadata(null);

        Assert.assertEquals(vitessDatabaseMetaData.doesMaxRowSizeIncludeBlobs(), false);
    }

    @Test public void getMaxTableNameLengthTest() throws SQLException {
        VitessDatabaseMetaData vitessDatabaseMetaData = new VitessMySQLDatabaseMetadata(null);

        Assert.assertEquals(vitessDatabaseMetaData.getMaxTableNameLength(), 64);
    }

    @Test public void getMaxTablesInSelectTest() throws SQLException {
        VitessDatabaseMetaData vitessDatabaseMetaData = new VitessMySQLDatabaseMetadata(null);

        Assert.assertEquals(vitessDatabaseMetaData.getMaxTablesInSelect(), 256);
    }

    @Test public void getMaxUserNameLengthTest() throws SQLException {
        VitessDatabaseMetaData vitessDatabaseMetaData = new VitessMySQLDatabaseMetadata(null);

        Assert.assertEquals(vitessDatabaseMetaData.getMaxUserNameLength(), 16);
    }

    @Test public void supportsDataDefinitionAndDataManipulationTransactionsTest()
        throws SQLException {
        VitessDatabaseMetaData vitessDatabaseMetaData = new VitessMySQLDatabaseMetadata(null);

        Assert.assertEquals(
            vitessDatabaseMetaData.supportsDataDefinitionAndDataManipulationTransactions(), false);
    }

    @Test public void dataDefinitionCausesTransactionCommitTest() throws SQLException {
        VitessDatabaseMetaData vitessDatabaseMetaData = new VitessMySQLDatabaseMetadata(null);

        Assert.assertEquals(vitessDatabaseMetaData.dataDefinitionCausesTransactionCommit(), false);
    }

    @Test public void dataDefinitionIgnoredInTransactionsTest() throws SQLException {
        VitessDatabaseMetaData vitessDatabaseMetaData = new VitessMySQLDatabaseMetadata(null);

        Assert.assertEquals(vitessDatabaseMetaData.dataDefinitionIgnoredInTransactions(), false);
    }

    @Test public void getIdentifierQuoteStringTest() throws SQLException {
        VitessDatabaseMetaData vitessDatabaseMetaData = new VitessMySQLDatabaseMetadata(null);

        Assert.assertEquals(vitessDatabaseMetaData.getIdentifierQuoteString(), "`");
    }

    @Test public void getProceduresTest() throws SQLException {
        VitessDatabaseMetaData vitessDatabaseMetaData = new VitessMySQLDatabaseMetadata(null);

        Assert.assertEquals(vitessDatabaseMetaData.getProcedures(null, null, null), null);
    }

    @Test public void supportsResultSetTypeTest() throws SQLException {
        VitessDatabaseMetaData vitessDatabaseMetaData = new VitessMySQLDatabaseMetadata(null);

        Assert
            .assertEquals(vitessDatabaseMetaData.supportsResultSetType(ResultSet.TYPE_FORWARD_ONLY),
                true);
        Assert.assertEquals(
            vitessDatabaseMetaData.supportsResultSetType(ResultSet.TYPE_SCROLL_INSENSITIVE), false);
        Assert.assertEquals(
            vitessDatabaseMetaData.supportsResultSetType(ResultSet.TYPE_SCROLL_SENSITIVE), false);
        Assert.assertEquals(
            vitessDatabaseMetaData.supportsResultSetType(ResultSet.CLOSE_CURSORS_AT_COMMIT), false);
        Assert
            .assertEquals(vitessDatabaseMetaData.supportsResultSetType(ResultSet.CONCUR_READ_ONLY),
                false);
        Assert
            .assertEquals(vitessDatabaseMetaData.supportsResultSetType(ResultSet.CONCUR_UPDATABLE),
                false);
        Assert.assertEquals(vitessDatabaseMetaData.supportsResultSetType(ResultSet.FETCH_FORWARD),
            false);
        Assert.assertEquals(vitessDatabaseMetaData.supportsResultSetType(ResultSet.FETCH_REVERSE),
            false);
        Assert.assertEquals(
            vitessDatabaseMetaData.supportsResultSetType(ResultSet.HOLD_CURSORS_OVER_COMMIT),
            false);
        Assert.assertEquals(vitessDatabaseMetaData.supportsResultSetType(ResultSet.FETCH_UNKNOWN),
            false);
    }

    @Test public void supportsResultSetConcurrencyTest() throws SQLException {
        VitessDatabaseMetaData vitessDatabaseMetaData = new VitessMySQLDatabaseMetadata(null);
        Assert.assertEquals(vitessDatabaseMetaData
            .supportsResultSetConcurrency(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY),
            true);
        Assert.assertEquals(vitessDatabaseMetaData
            .supportsResultSetConcurrency(ResultSet.TYPE_SCROLL_INSENSITIVE,
                ResultSet.CONCUR_READ_ONLY), false);
        Assert.assertEquals(vitessDatabaseMetaData
            .supportsResultSetConcurrency(ResultSet.TYPE_SCROLL_SENSITIVE,
                ResultSet.CONCUR_READ_ONLY), false);
        Assert.assertEquals(vitessDatabaseMetaData
            .supportsResultSetConcurrency(ResultSet.CLOSE_CURSORS_AT_COMMIT,
                ResultSet.CONCUR_READ_ONLY), false);
        Assert.assertEquals(vitessDatabaseMetaData
            .supportsResultSetConcurrency(ResultSet.CONCUR_READ_ONLY, ResultSet.CONCUR_READ_ONLY),
            false);
        Assert.assertEquals(vitessDatabaseMetaData
            .supportsResultSetConcurrency(ResultSet.CONCUR_UPDATABLE, ResultSet.CONCUR_READ_ONLY),
            false);
        Assert.assertEquals(vitessDatabaseMetaData
                .supportsResultSetConcurrency(ResultSet.FETCH_FORWARD, ResultSet.CONCUR_READ_ONLY),
            false);
        Assert.assertEquals(vitessDatabaseMetaData
                .supportsResultSetConcurrency(ResultSet.FETCH_REVERSE, ResultSet.CONCUR_READ_ONLY),
            false);
        Assert.assertEquals(vitessDatabaseMetaData
            .supportsResultSetConcurrency(ResultSet.HOLD_CURSORS_OVER_COMMIT,
                ResultSet.CONCUR_READ_ONLY), false);
        Assert.assertEquals(vitessDatabaseMetaData
                .supportsResultSetConcurrency(ResultSet.FETCH_UNKNOWN, ResultSet.CONCUR_READ_ONLY),
            false);
    }

    @Test public void getJDBCMajorVersionTest() throws SQLException {
        VitessDatabaseMetaData vitessDatabaseMetaData = new VitessMySQLDatabaseMetadata(null);

        Assert.assertEquals(vitessDatabaseMetaData.getJDBCMajorVersion(), 1);
    }

    @Test public void getJDBCMinorVersionTest() throws SQLException {
        VitessDatabaseMetaData vitessDatabaseMetaData = new VitessMySQLDatabaseMetadata(null);

        Assert.assertEquals(vitessDatabaseMetaData.getJDBCMinorVersion(), 0);
    }

    @Test public void getNumericFunctionsTest() throws SQLException {
        VitessDatabaseMetaData vitessDatabaseMetaData = new VitessMySQLDatabaseMetadata(null);

        Assert.assertEquals(vitessDatabaseMetaData.getNumericFunctions(),
            "ABS,ACOS,ASIN,ATAN,ATAN2,BIT_COUNT,CEILING,COS,COT,DEGREES,EXP,FLOOR,LOG,LOG10,MAX,MIN,MOD,PI,POW,POWER,RADIANS,RAND,ROUND,SIN,SQRT,TAN,TRUNCATE");
    }

    @Test public void getStringFunctionsTest() throws SQLException {
        VitessDatabaseMetaData vitessDatabaseMetaData = new VitessMySQLDatabaseMetadata(null);

        Assert.assertEquals(vitessDatabaseMetaData.getStringFunctions(),
            "ASCII,BIN,BIT_LENGTH,CHAR,CHARACTER_LENGTH,CHAR_LENGTH,CONCAT,CONCAT_WS,CONV,ELT,EXPORT_SET,FIELD,FIND_IN_SET,HEX,INSERT,INSTR,LCASE,LEFT,LENGTH,LOAD_FILE,LOCATE,LOCATE,LOWER,LPAD,LTRIM,MAKE_SET,MATCH,MID,OCT,OCTET_LENGTH,ORD,POSITION,QUOTE,REPEAT,REPLACE,REVERSE,RIGHT,RPAD,RTRIM,SOUNDEX,SPACE,STRCMP,SUBSTRING,SUBSTRING,SUBSTRING,SUBSTRING,SUBSTRING_INDEX,TRIM,UCASE,UPPER");
    }

    @Test public void getSystemFunctionsTest() throws SQLException {
        VitessDatabaseMetaData vitessMySQLDatabaseMetadata = new VitessMySQLDatabaseMetadata(null);
        VitessDatabaseMetaData vitessMariaDBDatbaseMetadata =
            new VitessMariaDBDatabaseMetadata(null);

        Assert.assertEquals(vitessMySQLDatabaseMetadata.getSystemFunctions(),
            "DATABASE,USER,SYSTEM_USER,SESSION_USER,LAST_INSERT_ID,VERSION,PASSWORD,ENCRYPT");
        Assert.assertEquals(vitessMariaDBDatbaseMetadata.getSystemFunctions(),
            "DATABASE,USER,SYSTEM_USER,SESSION_USER,LAST_INSERT_ID,VERSION");
    }

    @Test public void getTimeDateFunctionsTest() throws SQLException {
        VitessDatabaseMetaData vitessDatabaseMetaData = new VitessMySQLDatabaseMetadata(null);

        Assert.assertEquals(vitessDatabaseMetaData.getTimeDateFunctions(),
            "DAYOFWEEK,WEEKDAY,DAYOFMONTH,DAYOFYEAR,MONTH,DAYNAME,MONTHNAME,QUARTER,WEEK,YEAR,HOUR,MINUTE,SECOND,PERIOD_ADD,PERIOD_DIFF,TO_DAYS,FROM_DAYS,DATE_FORMAT,TIME_FORMAT,CURDATE,CURRENT_DATE,CURTIME,CURRENT_TIME,NOW,SYSDATE,CURRENT_TIMESTAMP,UNIX_TIMESTAMP,FROM_UNIXTIME,SEC_TO_TIME,TIME_TO_SEC");
    }

    @Test public void autoCommitFailureClosesAllResultSetsTest() throws SQLException {
        VitessDatabaseMetaData vitessDatabaseMetaData = new VitessMySQLDatabaseMetadata(null);

        Assert.assertEquals(vitessDatabaseMetaData.autoCommitFailureClosesAllResultSets(), false);
    }

    @Test public void getUrlTest() throws SQLException {
        VitessConnection mockConn = PowerMockito.mock(VitessConnection.class);
        String connectionUrl = "jdbc:vitess://<vtGateHostname>:<vtGatePort>/<keyspace>/<dbName>";
        PowerMockito.when(mockConn.getUrl()).thenReturn(connectionUrl);
        Assert.assertEquals(mockConn.getUrl(), connectionUrl);

    }

    @Test public void isReadOnlyTest() throws SQLException {
        VitessConnection mockConn = PowerMockito.mock(VitessConnection.class);
        PowerMockito.when(mockConn.isReadOnly()).thenReturn(false);
        Assert.assertEquals(mockConn.isReadOnly(), false);

    }

    @Test public void getDriverNameTest() throws SQLException {
        VitessDatabaseMetaData vitessMySQLDatabaseMetadata = new VitessMySQLDatabaseMetadata(null);
        VitessDatabaseMetaData vitessMariaDBDatabaseMetadata =
            new VitessMariaDBDatabaseMetadata(null);

        Assert
            .assertEquals(vitessMySQLDatabaseMetadata.getDriverName(), "Vitess MySQL JDBC Driver");
        Assert.assertEquals(vitessMariaDBDatabaseMetadata.getDriverName(),
            "Vitess MariaDB JDBC Driver");
    }

    @Test public void usesLocalFilesTest() throws SQLException {
        VitessDatabaseMetaData vitessDatabaseMetaData = new VitessMySQLDatabaseMetadata(null);

        Assert.assertEquals(vitessDatabaseMetaData.usesLocalFiles(), false);
        Assert.assertEquals(vitessDatabaseMetaData.usesLocalFilePerTable(), false);
        Assert.assertEquals(vitessDatabaseMetaData.storesUpperCaseIdentifiers(), false);
    }

    @Test public void storeIdentifiersTest() throws SQLException {
        VitessDatabaseMetaData vitessDatabaseMetaData = new VitessMySQLDatabaseMetadata(null);

        Assert.assertEquals(vitessDatabaseMetaData.storesUpperCaseIdentifiers(), false);
    }

    @Test public void supportsTransactionsTest() throws SQLException {
        VitessDatabaseMetaData vitessDatabaseMetaData = new VitessMySQLDatabaseMetadata(null);
        Assert.assertEquals(vitessDatabaseMetaData.supportsTransactions(), true);
    }

    @Test public void supportsTransactionIsolationLevelTest() throws SQLException {
        VitessDatabaseMetaData vitessDatabaseMetaData = new VitessMySQLDatabaseMetadata(null);
        Assert.assertEquals(
            vitessDatabaseMetaData.supportsTransactionIsolationLevel(Connection.TRANSACTION_NONE),
            false);
        Assert.assertEquals(vitessDatabaseMetaData
            .supportsTransactionIsolationLevel(Connection.TRANSACTION_READ_COMMITTED), true);
        Assert.assertEquals(vitessDatabaseMetaData
            .supportsTransactionIsolationLevel(Connection.TRANSACTION_READ_UNCOMMITTED), true);
        Assert.assertEquals(vitessDatabaseMetaData
            .supportsTransactionIsolationLevel(Connection.TRANSACTION_REPEATABLE_READ), true);
        Assert.assertEquals(vitessDatabaseMetaData
            .supportsTransactionIsolationLevel(Connection.TRANSACTION_SERIALIZABLE), true);
    }

    @Test public void getMaxProcedureNameLengthTest() throws SQLException {
        VitessDatabaseMetaData vitessDatabaseMetaData = new VitessMySQLDatabaseMetadata(null);
        Assert.assertEquals(vitessDatabaseMetaData.getMaxProcedureNameLength(), 256);
    }

    @Test public void getMaxCatalogNameLengthTest() throws SQLException {
        VitessDatabaseMetaData vitessMySqlDatabaseMetaData = new VitessMySQLDatabaseMetadata(null);
        VitessDatabaseMetaData vitessMariaDBDatabaseMetadata =
            new VitessMariaDBDatabaseMetadata(null);

        Assert.assertEquals(vitessMySqlDatabaseMetaData.getMaxCatalogNameLength(), 32);
        Assert.assertEquals(vitessMariaDBDatabaseMetadata.getMaxCatalogNameLength(), 0);
    }

    @Test public void getMaxRowSizeTest() throws SQLException {
        VitessDatabaseMetaData vitessMySqlDatabaseMetaData = new VitessMySQLDatabaseMetadata(null);
        VitessDatabaseMetaData vitessMariaDBDatabaseMetadata =
            new VitessMariaDBDatabaseMetadata(null);

        Assert.assertEquals(vitessMySqlDatabaseMetaData.getMaxRowSize(), 2147483639);
        Assert.assertEquals(vitessMariaDBDatabaseMetadata.getMaxRowSize(), 0);
    }

    @Test public void getMaxStatementLengthTest() throws SQLException {
        VitessDatabaseMetaData vitessMySqlDatabaseMetaData = new VitessMySQLDatabaseMetadata(null);
        VitessDatabaseMetaData vitessMariaDBDatabaseMetadata =
            new VitessMariaDBDatabaseMetadata(null);

        Assert.assertEquals(vitessMySqlDatabaseMetaData.getMaxStatementLength(), 65531);
        Assert.assertEquals(vitessMariaDBDatabaseMetadata.getMaxStatementLength(), 0);
    }

    @Test public void getMaxStatementsTest() throws SQLException {
        VitessDatabaseMetaData vitessMySqlDatabaseMetaData = new VitessMySQLDatabaseMetadata(null);
        VitessDatabaseMetaData vitessMariaDBDatabaseMetadata =
            new VitessMariaDBDatabaseMetadata(null);

        Assert.assertEquals(vitessMySqlDatabaseMetaData.getMaxStatements(), 0);
        Assert.assertEquals(vitessMariaDBDatabaseMetadata.getMaxStatements(), 0);
    }

    @Test public void supportsDataManipulationTransactionsOnlyTest() throws SQLException {
        VitessDatabaseMetaData vitessMySqlDatabaseMetaData = new VitessMySQLDatabaseMetadata(null);
        VitessDatabaseMetaData vitessMariaDBDatabaseMetadata =
            new VitessMariaDBDatabaseMetadata(null);

        Assert.assertEquals(vitessMySqlDatabaseMetaData.supportsDataManipulationTransactionsOnly(),
            false);
        Assert
            .assertEquals(vitessMariaDBDatabaseMetadata.supportsDataManipulationTransactionsOnly(),
                false);
    }

    @Test public void getMaxSchemaNameLengthTest() throws SQLException {
        VitessDatabaseMetaData vitessMySqlDatabaseMetaData = new VitessMySQLDatabaseMetadata(null);
        VitessDatabaseMetaData vitessMariaDBDatabaseMetadata =
            new VitessMariaDBDatabaseMetadata(null);

        Assert.assertEquals(vitessMySqlDatabaseMetaData.getMaxSchemaNameLength(), 0);
        Assert.assertEquals(vitessMariaDBDatabaseMetadata.getMaxSchemaNameLength(), 32);
    }

    @Test public void supportsSavepointsTest() throws SQLException {
        VitessDatabaseMetaData vitessMySqlDatabaseMetaData = new VitessMySQLDatabaseMetadata(null);
        VitessDatabaseMetaData vitessMariaDBDatabaseMetadata =
            new VitessMariaDBDatabaseMetadata(null);

        Assert.assertEquals(vitessMySqlDatabaseMetaData.supportsSavepoints(), false);
        Assert.assertEquals(vitessMariaDBDatabaseMetadata.supportsSavepoints(), false);
    }

    @Test public void supportsMultipleOpenResultsTest() throws SQLException {
        VitessDatabaseMetaData vitessMySqlDatabaseMetaData = new VitessMySQLDatabaseMetadata(null);
        VitessDatabaseMetaData vitessMariaDBDatabaseMetadata =
            new VitessMariaDBDatabaseMetadata(null);

        Assert.assertEquals(vitessMySqlDatabaseMetaData.supportsMultipleOpenResults(), false);
        Assert.assertEquals(vitessMariaDBDatabaseMetadata.supportsMultipleOpenResults(), false);
    }

    @Test public void locatorsUpdateCopyTest() throws SQLException {
        VitessDatabaseMetaData vitessMySqlDatabaseMetaData = new VitessMySQLDatabaseMetadata(null);
        VitessDatabaseMetaData vitessMariaDBDatabaseMetadata =
            new VitessMariaDBDatabaseMetadata(null);

        Assert.assertEquals(vitessMySqlDatabaseMetaData.locatorsUpdateCopy(), true);
        Assert.assertEquals(vitessMariaDBDatabaseMetadata.locatorsUpdateCopy(), false);
    }

    @Test public void supportsStatementPooling() throws SQLException {
        VitessDatabaseMetaData vitessMySqlDatabaseMetaData = new VitessMySQLDatabaseMetadata(null);
        VitessDatabaseMetaData vitessMariaDBDatabaseMetadata =
            new VitessMariaDBDatabaseMetadata(null);

        Assert.assertEquals(vitessMySqlDatabaseMetaData.supportsStatementPooling(), false);
        Assert.assertEquals(vitessMariaDBDatabaseMetadata.supportsStatementPooling(), false);
    }

    @Test public void getCatalogsTest() throws SQLException, Exception {
        String sql = "SHOW DATABASES";
        Cursor mockedCursor = new SimpleCursor(Query.QueryResult.newBuilder().addFields(
            Query.Field.newBuilder().setName("TABLE_CAT").setType(Query.Type.VARCHAR).build())
            .addRows(Query.Row.newBuilder().addLengths("vitessDB".length())
                .setValues(ByteString.copyFromUtf8("vitessDB"))).addRows(
                Query.Row.newBuilder().addLengths("sampleDB".length())
                    .setValues(ByteString.copyFromUtf8("sampleDB"))).addRows(
                Query.Row.newBuilder().addLengths("testDB".length())
                    .setValues(ByteString.copyFromUtf8("testDB"))).addRows(
                Query.Row.newBuilder().addLengths("dummyDB".length())
                    .setValues(ByteString.copyFromUtf8("dummyDB"))).build());

        VitessStatement vitessStatement = PowerMockito.mock(VitessStatement.class);
        PowerMockito.whenNew(VitessStatement.class).withAnyArguments().thenReturn(vitessStatement);
        PowerMockito.when(vitessStatement.executeQuery(sql))
            .thenReturn(new VitessResultSet(mockedCursor));

        VitessDatabaseMetaData vitessDatabaseMetaData = new VitessMySQLDatabaseMetadata(null);
        ResultSet resultSet = vitessDatabaseMetaData.getCatalogs();
        ArrayList<String> resultSetList = new ArrayList<String>();
        while (resultSet.next()) {
            resultSetList.add(resultSet.getString(1));
        }
        Assert.assertEquals(resultSetList.get(0), "dummyDB");
        Assert.assertEquals(resultSetList.get(1), "sampleDB");
        Assert.assertEquals(resultSetList.get(2), "testDB");
        Assert.assertEquals(resultSetList.get(3), "vitessDB");
    }

    @Test public void getTablesTest() throws SQLException, Exception {

        String sql = "SHOW FULL TABLES FROM `vt` LIKE '%'";
        Cursor mockedCursor = new SimpleCursor(Query.QueryResult.newBuilder()
            .addFields(Query.Field.newBuilder().setName("TABLE_CAT").setType(Query.Type.VARCHAR))
            .addFields(Query.Field.newBuilder().setName("TABLE_SCHEM").setType(Query.Type.VARCHAR))
            .addFields(Query.Field.newBuilder().setName("TABLE_NAME").setType(Query.Type.VARCHAR))
            .addFields(Query.Field.newBuilder().setName("TABLE_TYPE").setType(Query.Type.VARCHAR))
            .addFields(Query.Field.newBuilder().setName("REMARKS").setType(Query.Type.VARCHAR))
            .addFields(Query.Field.newBuilder().setName("TYPE_CAT").setType(Query.Type.VARCHAR))
            .addFields(Query.Field.newBuilder().setName("TYPE_SCHEM").setType(Query.Type.VARCHAR))
            .addFields(Query.Field.newBuilder().setName("TYPE_NAME").setType(Query.Type.VARCHAR))
            .addFields(Query.Field.newBuilder().setName("SELF_REFERENCING_COL_NAME")
                .setType(Query.Type.VARCHAR)).addFields(
                Query.Field.newBuilder().setName("REF_GENERATION").setType(Query.Type.VARCHAR))
            .addRows(Query.Row.newBuilder().addLengths("TestDB1".length()).addLengths("".length())
                .addLengths("SampleTable1".length()).addLengths("TABLE".length())
                .addLengths("".length()).addLengths("".length()).addLengths("".length())
                .addLengths("".length()).addLengths("".length()).addLengths("".length())
                .setValues(ByteString.copyFromUtf8("TestDB1sampleTable1TABLE"))).addRows(
                Query.Row.newBuilder().addLengths("TestDB1".length()).addLengths("".length())
                    .addLengths("SampleView1".length()).addLengths("VIEW".length())
                    .addLengths("".length()).addLengths("".length()).addLengths("".length())
                    .addLengths("".length()).addLengths("".length()).addLengths("".length())
                    .setValues(ByteString.copyFromUtf8("TestDB1SampleView1VIEW"))).addRows(
                Query.Row.newBuilder().addLengths("TestDB1".length()).addLengths("".length())
                    .addLengths("SampleSystemView".length()).addLengths("SYSTEM VIEW".length())
                    .addLengths("".length()).addLengths("".length()).addLengths("".length())
                    .addLengths("".length()).addLengths("".length()).addLengths("".length())
                    .setValues(ByteString.copyFromUtf8("TestDB2SampleSystemViewSYSTEM VIEW")))
            .addRows(Query.Row.newBuilder().addLengths("TestDB1".length()).addLengths("".length())
                .addLengths("SampleSystemTable".length()).addLengths("SYSTEM TABLE".length())
                .addLengths("".length()).addLengths("".length()).addLengths("".length())
                .addLengths("".length()).addLengths("".length()).addLengths("".length())
                .setValues(ByteString.copyFromUtf8("TestDB2SampleSystemTableSYSTEM TABLE")))
            .addRows(Query.Row.newBuilder().addLengths("TestDB1".length()).addLengths("".length())
                .addLengths("SampleLocalTemporary".length()).addLengths("LOCAL TEMPORARY".length())
                .addLengths("".length()).addLengths("".length()).addLengths("".length())
                .addLengths("".length()).addLengths("".length()).addLengths("".length())
                .setValues(ByteString.copyFromUtf8("TestDB2SampleLocalTemporaryLOCAL TEMPORARY")))
            .build());

        Cursor actualCursor = new SimpleCursor(Query.QueryResult.newBuilder()
            .addFields(Query.Field.newBuilder().setName("TABLE_CAT").setType(Query.Type.VARCHAR))
            .addFields(Query.Field.newBuilder().setName("TABLE_SCHEM").setType(Query.Type.VARCHAR))
            .addFields(Query.Field.newBuilder().setName("TABLE_NAME").setType(Query.Type.VARCHAR))
            .addFields(Query.Field.newBuilder().setName("TABLE_TYPE").setType(Query.Type.VARCHAR))
            .addFields(Query.Field.newBuilder().setName("REMARKS").setType(Query.Type.VARCHAR))
            .addFields(Query.Field.newBuilder().setName("TYPE_CAT").setType(Query.Type.VARCHAR))
            .addFields(Query.Field.newBuilder().setName("TYPE_SCHEM").setType(Query.Type.VARCHAR))
            .addFields(Query.Field.newBuilder().setName("TYPE_NAME").setType(Query.Type.VARCHAR))
            .addFields(Query.Field.newBuilder().setName("SELF_REFERENCING_COL_NAME")
                .setType(Query.Type.VARCHAR)).addFields(
                Query.Field.newBuilder().setName("REF_GENERATION").setType(Query.Type.VARCHAR))
            .addRows(Query.Row.newBuilder().addLengths("TestDB1".length()).addLengths("".length())
                .addLengths("SampleTable1".length()).addLengths("TABLE".length())
                .addLengths("".length()).addLengths("".length()).addLengths("".length())
                .addLengths("".length()).addLengths("".length()).addLengths("".length())
                .setValues(ByteString.copyFromUtf8("TestDB1sampleTable1TABLE"))).addRows(
                Query.Row.newBuilder().addLengths("TestDB1".length()).addLengths("".length())
                    .addLengths("SampleView1".length()).addLengths("VIEW".length())
                    .addLengths("".length()).addLengths("".length()).addLengths("".length())
                    .addLengths("".length()).addLengths("".length()).addLengths("".length())
                    .setValues(ByteString.copyFromUtf8("TestDB1SampleView1VIEW"))).addRows(
                Query.Row.newBuilder().addLengths("TestDB1".length()).addLengths("".length())
                    .addLengths("SampleSystemView".length()).addLengths("SYSTEM VIEW".length())
                    .addLengths("".length()).addLengths("".length()).addLengths("".length())
                    .addLengths("".length()).addLengths("".length()).addLengths("".length())
                    .setValues(ByteString.copyFromUtf8("TestDB2SampleSystemViewSYSTEM VIEW")))
            .addRows(Query.Row.newBuilder().addLengths("TestDB1".length()).addLengths("".length())
                .addLengths("SampleSystemTable".length()).addLengths("SYSTEM TABLE".length())
                .addLengths("".length()).addLengths("".length()).addLengths("".length())
                .addLengths("".length()).addLengths("".length()).addLengths("".length())
                .setValues(ByteString.copyFromUtf8("TestDB2SampleSystemTableSYSTEM TABLE")))
            .addRows(Query.Row.newBuilder().addLengths("TestDB1".length()).addLengths("".length())
                .addLengths("SampleLocalTemporary".length()).addLengths("LOCAL TEMPORARY".length())
                .addLengths("".length()).addLengths("".length()).addLengths("".length())
                .addLengths("".length()).addLengths("".length()).addLengths("".length())
                .setValues(ByteString.copyFromUtf8("TestDB2SampleLocalTemporaryLOCAL TEMPORARY")))
            .build());

        VitessStatement vitessStatement = PowerMockito.mock(VitessStatement.class);
        PowerMockito.whenNew(VitessStatement.class).withAnyArguments().thenReturn(vitessStatement);
        PowerMockito.when(vitessStatement.executeQuery(sql))
            .thenReturn(new VitessResultSet(mockedCursor));

        VitessDatabaseMetaData vitessDatabaseMetaData = new VitessMySQLDatabaseMetadata(null);
        ResultSet actualResultSet = vitessDatabaseMetaData.getTables("vt", null, null, null);
        ResultSet expectedResultSet = new VitessResultSet(mockedCursor);

        Assert.assertEquals(compareResultSets(actualResultSet, expectedResultSet), true);
    }

    @Test public void getColumnsTest() throws SQLException, Exception {

        String sql = "SHOW FULL COLUMNS FROM `sampleTable1` FROM `TestDB1` LIKE '%'";
        Cursor mockedTablecursor = new SimpleCursor(Query.QueryResult.newBuilder()
            .addFields(Query.Field.newBuilder().setName("TABLE_CAT").setType(Query.Type.VARCHAR))
            .addFields(Query.Field.newBuilder().setName("TABLE_SCHEM").setType(Query.Type.VARCHAR))
            .addFields(Query.Field.newBuilder().setName("TABLE_NAME").setType(Query.Type.VARCHAR))
            .addFields(Query.Field.newBuilder().setName("TABLE_TYPE").setType(Query.Type.VARCHAR))
            .addFields(Query.Field.newBuilder().setName("REMARKS").setType(Query.Type.VARCHAR))
            .addFields(Query.Field.newBuilder().setName("TYPE_CAT").setType(Query.Type.VARCHAR))
            .addFields(Query.Field.newBuilder().setName("TYPE_SCHEM").setType(Query.Type.VARCHAR))
            .addFields(Query.Field.newBuilder().setName("TYPE_NAME").setType(Query.Type.VARCHAR))
            .addFields(Query.Field.newBuilder().setName("SELF_REFERENCING_COL_NAME")
                .setType(Query.Type.VARCHAR)).addFields(
                Query.Field.newBuilder().setName("REF_GENERATION").setType(Query.Type.VARCHAR))
            .addRows(Query.Row.newBuilder().addLengths("TestDB1".length()).addLengths("".length())
                .addLengths("sampleTable1".length()).addLengths("TABLE".length())
                .addLengths("".length()).addLengths("".length()).addLengths("".length())
                .addLengths("".length()).addLengths("".length()).addLengths("".length())
                .setValues(ByteString.copyFromUtf8("TestDB1sampleTable1TABLE"))).build());

        Cursor actualCursor = new SimpleCursor(Query.QueryResult.newBuilder()
            .addFields(Query.Field.newBuilder().setName("Field").setType(Query.Type.VARCHAR))
            .addFields(Query.Field.newBuilder().setName("Type").setType(Query.Type.VARCHAR))
            .addFields(Query.Field.newBuilder().setName("Collation").setType(Query.Type.VARCHAR))
            .addFields(Query.Field.newBuilder().setName("Null").setType(Query.Type.VARCHAR))
            .addFields(Query.Field.newBuilder().setName("Key").setType(Query.Type.VARCHAR))
            .addFields(Query.Field.newBuilder().setName("Default").setType(Query.Type.VARCHAR))
            .addFields(Query.Field.newBuilder().setName("Extra").setType(Query.Type.VARCHAR))
            .addFields(Query.Field.newBuilder().setName("Privileges").setType(Query.Type.VARCHAR))
            .addFields(Query.Field.newBuilder().setName("Comment").setType(Query.Type.VARCHAR))
            .addRows(Query.Row.newBuilder().addLengths("shipmentid".length())
                .addLengths("bigint".length()).addLengths("NULL".length()).addLengths("NO".length())
                .addLengths("PRI".length()).addLengths("NULL".length()).addLengths("".length())
                .addLengths("select,insert,update,references".length()).addLengths("".length())
                .setValues(ByteString
                    .copyFromUtf8("shipmentidbigintNULLNOPRINULLselect,insert,update,references")))
            .addRows(Query.Row.newBuilder().addLengths("trackingid".length())
                .addLengths("varchar".length()).addLengths("utf8_general_ci".length())
                .addLengths("YES".length()).addLengths("".length()).addLengths("NULL".length())
                .addLengths("".length()).addLengths("select,insert,update,references".length())
                .addLengths("".length()).setValues(ByteString.copyFromUtf8(
                    "trackingidvarcharutf8_general_ciYESNULLselect,insert,update,references")))
            .build());
        Cursor expectedCursor = new SimpleCursor(Query.QueryResult.newBuilder()
            .addFields(Query.Field.newBuilder().setName("TABLE_CAT").setType(Query.Type.CHAR))
            .addFields(Query.Field.newBuilder().setName("TABLE_SCHEM").setType(Query.Type.CHAR))
            .addFields(Query.Field.newBuilder().setName("TABLE_NAME").setType(Query.Type.CHAR))
            .addFields(Query.Field.newBuilder().setName("COLUMN_NAME").setType(Query.Type.CHAR))
            .addFields(Query.Field.newBuilder().setName("DATA_TYPE").setType(Query.Type.INT32))
            .addFields(Query.Field.newBuilder().setName("TYPE_NAME").setType(Query.Type.CHAR))
            .addFields(Query.Field.newBuilder().setName("COLUMN_SIZE").setType(Query.Type.INT32))
            .addFields(Query.Field.newBuilder().setName("BUFFER_LENGTH").setType(Query.Type.INT32))
            .addFields(Query.Field.newBuilder().setName("DECIMAL_DIGITS").setType(Query.Type.INT32))
            .addFields(Query.Field.newBuilder().setName("NUM_PREC_RADIX").setType(Query.Type.INT32))
            .addFields(Query.Field.newBuilder().setName("NULLABLE").setType(Query.Type.INT32))
            .addFields(Query.Field.newBuilder().setName("REMARKS").setType(Query.Type.CHAR))
            .addFields(Query.Field.newBuilder().setName("COLUMN_DEF").setType(Query.Type.CHAR))
            .addFields(Query.Field.newBuilder().setName("SQL_DATA_TYPE").setType(Query.Type.INT32))
            .addFields(
                Query.Field.newBuilder().setName("SQL_DATETIME_SUB").setType(Query.Type.INT32))
            .addFields(
                Query.Field.newBuilder().setName("CHAR_OCTET_LENGTH").setType(Query.Type.INT32))
            .addFields(
                Query.Field.newBuilder().setName("ORDINAL_POSITION").setType(Query.Type.INT32))
            .addFields(Query.Field.newBuilder().setName("ISNULLABLE").setType(Query.Type.CHAR))
            .addFields(Query.Field.newBuilder().setName("SCOPE_CATALOG").setType(Query.Type.CHAR))
            .addFields(Query.Field.newBuilder().setName("SCOPE_SCHEMA").setType(Query.Type.CHAR))
            .addFields(Query.Field.newBuilder().setName("SCOPE_TABLE").setType(Query.Type.CHAR))
            .addFields(
                Query.Field.newBuilder().setName("SOURCE_DATA_TYPE").setType(Query.Type.INT16))
            .addFields(
                Query.Field.newBuilder().setName("IS_AUTOINCREMENT").setType(Query.Type.CHAR))
            .addFields(
                Query.Field.newBuilder().setName("IS_GENERATEDCOLUMN").setType(Query.Type.CHAR))
            .addRows(Query.Row.newBuilder().addLengths("TestDB1".length()).addLengths(-1)
                .addLengths("sampleTable1".length()).addLengths("shipmentid".length())
                .addLengths("-5".length()).addLengths("BIGINT".length()).addLengths("19".length())
                .addLengths("65535".length()).addLengths("0".length()).addLengths("10".length())
                .addLengths("0".length()).addLengths("Comment".length()).addLengths("NULL".length())
                .addLengths("0".length()).addLengths("0".length()).addLengths("0".length())
                .addLengths("1".length()).addLengths("NO".length()).addLengths(-1).addLengths(-1)
                .addLengths(-1).addLengths(-1).addLengths("NO".length()).addLengths("NO".length())
                .setValues(ByteString.copyFromUtf8(
                    "TestDB1sampleTable1shipmentid-5BIGINT19655350100CommentNULL0001NONONO")))
            .addRows(Query.Row.newBuilder().addLengths("TestDB1".length()).addLengths(-1)
                .addLengths("sampleTable1".length()).addLengths("trackingid".length())
                .addLengths("12".length()).addLengths("VARCHAR".length()).addLengths("255".length())
                .addLengths("65535".length()).addLengths(-1).addLengths("10".length())
                .addLengths("1".length()).addLengths("Comment".length()).addLengths("NULL".length())
                .addLengths("0".length()).addLengths("0".length()).addLengths("255".length())
                .addLengths("2".length()).addLengths("YES".length()).addLengths(-1).addLengths(-1)
                .addLengths(-1).addLengths(-1).addLengths("NO".length()).addLengths("NO".length())
                .setValues(ByteString.copyFromUtf8(
                    "TestDB1sampleTable1trackingid12VARCHAR25565535101CommentNULL002552YESNONO")))
            .build());

        VitessStatement vitessStatement = PowerMockito.mock(VitessStatement.class);
        PowerMockito.whenNew(VitessStatement.class).withAnyArguments().thenReturn(vitessStatement);
        PowerMockito.when(vitessStatement.executeQuery(sql))
            .thenReturn(new VitessResultSet(actualCursor));

        VitessDatabaseMetaData vitessDatabaseMetaData =
            PowerMockito.mock(VitessMySQLDatabaseMetadata.class);
        PowerMockito.doCallRealMethod().when(vitessDatabaseMetaData)
            .getColumns("TestDB1", null, null, null);
        PowerMockito.when(vitessDatabaseMetaData.getTables("TestDB1", null, "%", new String[0]))
            .thenReturn(new VitessResultSet(mockedTablecursor));
        ResultSet actualResultSet = vitessDatabaseMetaData.getColumns("TestDB1", null, null, null);
        ResultSet expectedResultSet = new VitessResultSet(expectedCursor);

        Assert.assertEquals(compareResultSets(actualResultSet, expectedResultSet), true);
    }

    @Test public void getPrimaryKeysTest() throws SQLException, Exception {

        String sql = "SHOW KEYS FROM `shipment` FROM `vt`";
        Cursor mockedCursor = new SimpleCursor(Query.QueryResult.newBuilder()
            .addFields(Query.Field.newBuilder().setName("TABLE").setType(Query.Type.VARCHAR))
            .addFields(Query.Field.newBuilder().setName("Non_unique").setType(Query.Type.INT64))
            .addFields(Query.Field.newBuilder().setName("Key_name").setType(Query.Type.VARCHAR))
            .addFields(Query.Field.newBuilder().setName("Seq_in_index").setType(Query.Type.INT64))
            .addFields(Query.Field.newBuilder().setName("Column_name").setType(Query.Type.VARCHAR))
            .addFields(Query.Field.newBuilder().setName("Collation").setType(Query.Type.VARCHAR))
            .addFields(Query.Field.newBuilder().setName("Cardinality").setType(Query.Type.INT64))
            .addFields(Query.Field.newBuilder().setName("Sub_part").setType(Query.Type.INT64))
            .addFields(Query.Field.newBuilder().setName("Packed").setType(Query.Type.VARCHAR))
            .addFields(Query.Field.newBuilder().setName("Null").setType(Query.Type.VARCHAR))
            .addFields(Query.Field.newBuilder().setName("Index_type").setType(Query.Type.VARCHAR))
            .addFields(Query.Field.newBuilder().setName("Comment").setType(Query.Type.VARCHAR))
            .addFields(
                Query.Field.newBuilder().setName("Index_comment").setType(Query.Type.VARCHAR))
            .addRows(Query.Row.newBuilder().addLengths("shipment".length()).addLengths("0".length())
                .addLengths("PRIMARY".length()).addLengths("1".length())
                .addLengths("shipmentid".length()).addLengths("A".length())
                .addLengths("434880".length()).addLengths(-1).addLengths(-1).addLengths("".length())
                .addLengths("BTREE".length()).addLengths("".length()).addLengths("".length())
                .setValues(ByteString.copyFromUtf8("shipment0PRIMARY1shipmentidA434880BTREE")))
            .build());
        Cursor expectedcursor = new SimpleCursor(Query.QueryResult.newBuilder()
            .addFields(Query.Field.newBuilder().setName("TABLE_CAT").setType(Query.Type.CHAR))
            .addFields(Query.Field.newBuilder().setName("TABLE_SCHEM").setType(Query.Type.CHAR))
            .addFields(Query.Field.newBuilder().setName("TABLE_NAME").setType(Query.Type.CHAR))
            .addFields(Query.Field.newBuilder().setName("COLUMN_NAME").setType(Query.Type.CHAR))
            .addFields(Query.Field.newBuilder().setName("KEY_SEQ").setType(Query.Type.INT16))
            .addFields(Query.Field.newBuilder().setName("PK_NAME").setType(Query.Type.CHAR))
            .addRows(Query.Row.newBuilder().addLengths("vt".length()).addLengths(-1)
                .addLengths("shipment".length()).addLengths("shipmentid".length())
                .addLengths("1".length()).addLengths("PRIMARY".length())
                .setValues(ByteString.copyFromUtf8("vtshipmentshipmentid1PRIMARY"))).build());

        VitessStatement vitessStatement = PowerMockito.mock(VitessStatement.class);
        VitessDatabaseMetaData vitessDatabaseMetaData =
            PowerMockito.mock(VitessMySQLDatabaseMetadata.class);
        PowerMockito.mock(VitessMySQLDatabaseMetadata.class);
        PowerMockito.doCallRealMethod().when(vitessDatabaseMetaData)
            .getPrimaryKeys("vt", null, "shipment");
        PowerMockito.whenNew(VitessStatement.class).withAnyArguments().thenReturn(vitessStatement);
        PowerMockito.when(vitessStatement.executeQuery(sql))
            .thenReturn(new VitessResultSet(mockedCursor));
        ResultSet expectedResultSet = vitessDatabaseMetaData.getPrimaryKeys("vt", null, "shipment");
        ResultSet actualResultSet = new VitessResultSet(expectedcursor);

        Assert.assertEquals(compareResultSets(actualResultSet, expectedResultSet), true);
    }

    @Test public void getIndexInfoTest() throws SQLException, Exception {

        String sql = "SHOW INDEX FROM `shipment` FROM `vt`";
        Cursor mockedCursor = new SimpleCursor(Query.QueryResult.newBuilder()
            .addFields(Query.Field.newBuilder().setName("Table").setType(Query.Type.VARCHAR))
            .addFields(Query.Field.newBuilder().setName("Non_unique").setType(Query.Type.INT64))
            .addFields(Query.Field.newBuilder().setName("Key_name").setType(Query.Type.VARCHAR))
            .addFields(Query.Field.newBuilder().setName("Seq_in_index").setType(Query.Type.INT64))
            .addFields(Query.Field.newBuilder().setName("Column_name").setType(Query.Type.VARCHAR))
            .addFields(Query.Field.newBuilder().setName("Collation").setType(Query.Type.VARCHAR))
            .addFields(Query.Field.newBuilder().setName("Cardinality").setType(Query.Type.INT64))
            .addFields(Query.Field.newBuilder().setName("Sub_part").setType(Query.Type.INT64))
            .addFields(Query.Field.newBuilder().setName("Packed").setType(Query.Type.VARCHAR))
            .addFields(Query.Field.newBuilder().setName("Null").setType(Query.Type.VARCHAR))
            .addFields(Query.Field.newBuilder().setName("Index_type").setType(Query.Type.VARCHAR))
            .addFields(Query.Field.newBuilder().setName("Comment").setType(Query.Type.VARCHAR))
            .addFields(
                Query.Field.newBuilder().setName("Index_comment").setType(Query.Type.VARCHAR))
            .addRows(Query.Row.newBuilder().addLengths("shipment".length()).addLengths("0".length())
                .addLengths("PRIMARY".length()).addLengths("1".length())
                .addLengths("shipmentid".length()).addLengths("A".length())
                .addLengths("434880".length()).addLengths(-1).addLengths(-1).addLengths("".length())
                .addLengths("BTREE".length()).addLengths("".length()).addLengths("".length())
                .setValues(ByteString.copyFromUtf8("shipment0PRIMARY1shipmentidA434880BTREE")))
            .build());

        Cursor expectedcursor = new SimpleCursor(Query.QueryResult.newBuilder()
            .addFields(Query.Field.newBuilder().setName("TABLE_CAT").setType(Query.Type.CHAR))
            .addFields(Query.Field.newBuilder().setName("TABLE_SCHEM").setType(Query.Type.CHAR))
            .addFields(Query.Field.newBuilder().setName("TABLE_NAME").setType(Query.Type.CHAR))
            .addFields(Query.Field.newBuilder().setName("Non_unique").setType(Query.Type.BIT))
            .addFields(Query.Field.newBuilder().setName("INDEX_QUALIFIER").setType(Query.Type.CHAR))
            .addFields(Query.Field.newBuilder().setName("INDEX_NAME").setType(Query.Type.CHAR))
            .addFields(Query.Field.newBuilder().setName("TYPE").setType(Query.Type.INT16))
            .addFields(
                Query.Field.newBuilder().setName("ORDINAL_POSITION").setType(Query.Type.INT16))
            .addFields(Query.Field.newBuilder().setName("COLUMN_NAME").setType(Query.Type.CHAR))
            .addFields(Query.Field.newBuilder().setName("ASC_OR_DESC").setType(Query.Type.CHAR))
            .addFields(Query.Field.newBuilder().setName("CARDINALITY").setType(Query.Type.INT32))
            .addFields(Query.Field.newBuilder().setName("PAGES").setType(Query.Type.INT32))
            .addFields(
                Query.Field.newBuilder().setName("FILTER_CONDITION").setType(Query.Type.CHAR))
            .addRows(Query.Row.newBuilder().addLengths("vt".length()).addLengths(-1)
                .addLengths("shipment".length()).addLengths("false".length())
                .addLengths("".length()).addLengths("PRIMARY".length()).addLengths("3".length())
                .addLengths("1".length()).addLengths("shipmentid".length()).addLengths("A".length())
                .addLengths("434880".length()).addLengths("0".length()).addLengths(-1)
                .setValues(ByteString.copyFromUtf8("vtshipmentfalsePRIMARY31shipmentidA4348800")))
            .build());
        VitessStatement vitessStatement = PowerMockito.mock(VitessStatement.class);
        VitessDatabaseMetaData vitessDatabaseMetaData =
            PowerMockito.mock(VitessMySQLDatabaseMetadata.class);
        PowerMockito.mock(VitessMySQLDatabaseMetadata.class);
        PowerMockito.doCallRealMethod().when(vitessDatabaseMetaData)
            .getIndexInfo("vt", null, "shipment", true, false);
        PowerMockito.whenNew(VitessStatement.class).withAnyArguments().thenReturn(vitessStatement);
        PowerMockito.when(vitessStatement.executeQuery(sql))
            .thenReturn(new VitessResultSet(mockedCursor));
        ResultSet actualResultSet =
            vitessDatabaseMetaData.getIndexInfo("vt", null, "shipment", true, false);
        ResultSet expectedResultSet = new VitessResultSet(expectedcursor);

        Assert.assertEquals(compareResultSets(actualResultSet, expectedResultSet), true);
    }

    private boolean compareResultSets(ResultSet actualResultSet, ResultSet expectedResultSet)
        throws SQLException {
        boolean areResultSetsEqual = true;
        ResultSetMetaData actualResultSetMetadata = actualResultSet.getMetaData();
        ResultSetMetaData expectedResultSetMetadata = expectedResultSet.getMetaData();
        //Column Count Comparison
        if (actualResultSetMetadata.getColumnCount() != expectedResultSetMetadata
            .getColumnCount()) {
            areResultSetsEqual = false;
        }
        //Column Type Comparison
        if (areResultSetsEqual) {
            for (int i = 0; i < actualResultSetMetadata.getColumnCount(); i++) {
                if (actualResultSetMetadata.getColumnType(i + 1) != expectedResultSetMetadata
                    .getColumnType(i + 1)) {
                    areResultSetsEqual = false;
                    break;
                }
            }
        }
        //Actual Values Comparison
        if (areResultSetsEqual) {
            try {
                while (actualResultSet.next() && expectedResultSet.next()) {
                    for (int i = 0; i < actualResultSetMetadata.getColumnCount(); i++) {
                        switch (actualResultSetMetadata.getColumnType(i + 1)) {
                            case Types.TINYINT:
                            case Types.SMALLINT:
                            case Types.INTEGER:
                                if (actualResultSet.getInt(i + 1) != expectedResultSet
                                    .getInt(i + 1)) {
                                    return false;
                                }
                                break;
                            case Types.BIGINT:
                                if (actualResultSet.getLong(i + 1) != expectedResultSet
                                    .getLong(i + 1)) {
                                    return false;
                                }
                                break;
                            case Types.FLOAT:
                                if (actualResultSet.getFloat(i + 1) != expectedResultSet
                                    .getFloat(i + 1)) {
                                    return false;
                                }
                            case Types.DOUBLE:
                                if (actualResultSet.getDouble(i + 1) != expectedResultSet
                                    .getDouble(i + 1)) {
                                    return false;
                                }
                            case Types.TIME:
                                if (actualResultSet.getTime(i + 1) != expectedResultSet
                                    .getTime(i + 1)) {
                                    return false;
                                }
                                break;
                            case Types.TIMESTAMP:
                                if (actualResultSet.getTimestamp(i + 1) != expectedResultSet
                                    .getTimestamp(i + 1)) {
                                    return false;
                                }
                                break;
                            case Types.DATE:
                                if (actualResultSet.getDate(i + 1) != expectedResultSet
                                    .getDate(i + 1)) {
                                    return false;
                                }
                                break;
                            case Types.BLOB:
                                if (actualResultSet.getBlob(i + 1) != expectedResultSet
                                    .getBlob(i + 1)) {
                                    return false;
                                }
                                break;
                            case Types.BINARY:
                            case Types.LONGVARBINARY:
                                if (actualResultSet.getBytes(i + 1) != expectedResultSet
                                    .getBytes(i + 1)) {
                                    return false;
                                }
                                break;
                            default:
                                if (null == actualResultSet.getString(i + 1)
                                    && null == expectedResultSet.getString(i + 1))
                                    break;
                                if (!actualResultSet.getString(i + 1)
                                    .equals(expectedResultSet.getString(i + 1))) {
                                    return false;
                                }
                        }
                    }
                }

            } catch (SQLException ex) {
                areResultSetsEqual = false;
            }
        }
        return areResultSetsEqual;
    }

}
