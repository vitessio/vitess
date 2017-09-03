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

import java.sql.SQLException;
import java.sql.Types;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.mockito.internal.verification.VerificationModeFactory;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import io.vitess.proto.Query;
import io.vitess.util.MysqlDefs;
import io.vitess.util.charset.CharsetMapping;

@PrepareForTest(FieldWithMetadata.class)
@RunWith(PowerMockRunner.class)
public class FieldWithMetadataTest extends BaseTest {

    @Test
    public void testImplicitTempTable() throws SQLException {
        Query.Field raw = Query.Field.newBuilder()
            .setTable("#sql_my_temptable")
            .setCharset(CharsetMapping.MYSQL_COLLATION_INDEX_binary)
            .setFlags(Query.MySqlFlag.BINARY_FLAG_VALUE)
            .setType(Query.Type.VARCHAR)
            .setName("foo")
            .build();

        FieldWithMetadata fieldWithMetadata = new FieldWithMetadata(getVitessConnection(), raw);

        Assert.assertEquals(true, fieldWithMetadata.isImplicitTemporaryTable());
        Assert.assertEquals(false, fieldWithMetadata.isOpaqueBinary());

        VitessConnection conn = getVitessConnection();
        conn.setIncludedFields(Query.ExecuteOptions.IncludedFields.TYPE_AND_NAME);

        raw = Query.Field.newBuilder()
            .setType(Query.Type.VARCHAR)
            .setName("foo")
            .build();

        fieldWithMetadata = new FieldWithMetadata(conn, raw);

        Assert.assertEquals(false, fieldWithMetadata.isImplicitTemporaryTable());
        Assert.assertEquals(false, fieldWithMetadata.isOpaqueBinary());
    }

    @Test
    public void testBlobRemapping() throws SQLException {
        VitessConnection conn = getVitessConnection();
        conn.setBlobsAreStrings(true);

        Query.Field raw = Query.Field.newBuilder()
            .setTable("#sql_my_temptable")
            .setCharset(/* latin1, doesn't matter just dont want utf8 for now */ 5)
            .setFlags(Query.MySqlFlag.BINARY_FLAG_VALUE)
            .setType(Query.Type.BLOB)
            .setName("foo")
            .setOrgName("foo")
            .build();

        FieldWithMetadata fieldWithMetadata = new FieldWithMetadata(conn, raw);
        Assert.assertEquals(Types.VARCHAR, fieldWithMetadata.getJavaType());

        conn.setBlobsAreStrings(false);
        fieldWithMetadata = new FieldWithMetadata(conn, raw);
        Assert.assertEquals(Types.LONGVARCHAR, fieldWithMetadata.getJavaType());

        conn.setFunctionsNeverReturnBlobs(true);
        fieldWithMetadata = new FieldWithMetadata(conn, raw);
        Assert.assertEquals(Types.VARCHAR, fieldWithMetadata.getJavaType());

        conn.setFunctionsNeverReturnBlobs(false);
        conn.setUseBlobToStoreUTF8OutsideBMP(true);
        fieldWithMetadata = new FieldWithMetadata(conn, raw);
        Assert.assertEquals(Types.LONGVARCHAR, fieldWithMetadata.getJavaType());

        raw = raw.toBuilder()
            .setCharset(CharsetMapping.MYSQL_COLLATION_INDEX_binary)
            .setColumnLength(MysqlDefs.LENGTH_BLOB)
            .build();
        fieldWithMetadata = new FieldWithMetadata(conn, raw);
        Assert.assertEquals(Types.VARCHAR, fieldWithMetadata.getJavaType());
        Assert.assertEquals("utf8_general_ci", fieldWithMetadata.getCollation());

        conn.setUtf8OutsideBmpExcludedColumnNamePattern("^fo.*$");
        fieldWithMetadata = new FieldWithMetadata(conn, raw);
        Assert.assertEquals(Types.LONGVARBINARY, fieldWithMetadata.getJavaType());
        Assert.assertNotEquals("utf8_general_ci", fieldWithMetadata.getCollation());

        conn.setUtf8OutsideBmpIncludedColumnNamePattern("^foo$");
        fieldWithMetadata = new FieldWithMetadata(conn, raw);
        Assert.assertEquals(Types.VARCHAR, fieldWithMetadata.getJavaType());
        Assert.assertEquals("utf8_general_ci", fieldWithMetadata.getCollation());

        raw = raw.toBuilder()
            .setColumnLength(MysqlDefs.LENGTH_LONGBLOB)
            .build();
        fieldWithMetadata = new FieldWithMetadata(conn, raw);
        Assert.assertEquals(Types.LONGVARCHAR, fieldWithMetadata.getJavaType());
        Assert.assertEquals("utf8_general_ci", fieldWithMetadata.getCollation());

        conn.setUseBlobToStoreUTF8OutsideBMP(false);
        fieldWithMetadata = new FieldWithMetadata(conn, raw);
        Assert.assertEquals(Types.LONGVARBINARY, fieldWithMetadata.getJavaType());

        conn.setIncludedFields(Query.ExecuteOptions.IncludedFields.TYPE_AND_NAME);
        fieldWithMetadata = new FieldWithMetadata(conn, raw);
        Assert.assertEquals(Types.BLOB, fieldWithMetadata.getJavaType());
        Assert.assertEquals(null, fieldWithMetadata.getEncoding());
        Assert.assertEquals(null, fieldWithMetadata.getCollation());
    }

    @Test
    public void testTinyIntAsBit() throws SQLException {
        VitessConnection conn = getVitessConnection();

        Query.Field raw = Query.Field.newBuilder()
            .setTable("foo")
            .setColumnLength(3)
            .setType(Query.Type.INT8)
            .setName("foo")
            .setOrgName("foo")
            .build();
        conn.setTinyInt1isBit(true);
        FieldWithMetadata fieldWithMetadata = new FieldWithMetadata(conn, raw);
        Assert.assertEquals(Types.TINYINT, fieldWithMetadata.getJavaType());

        raw = raw.toBuilder()
            .setColumnLength(1)
            .build();
        fieldWithMetadata = new FieldWithMetadata(conn, raw);
        Assert.assertEquals(Types.BIT, fieldWithMetadata.getJavaType());
        conn.setIncludedFields(Query.ExecuteOptions.IncludedFields.TYPE_AND_NAME);
        fieldWithMetadata = new FieldWithMetadata(conn, raw);
        Assert.assertEquals(Types.TINYINT, fieldWithMetadata.getJavaType());
    }

    @Test
    public void testNonNumericNotDateTimeRemapping() throws SQLException {
        VitessConnection conn = getVitessConnection();

        Query.Field raw = Query.Field.newBuilder()
            .setTable("foo")
            .setColumnLength(3)
            .setType(Query.Type.VARBINARY)
            .setName("foo")
            .setOrgName("foo")
            .setCharset(/* utf-16 UnicodeBig */35)
            .build();

        FieldWithMetadata fieldWithMetadata = new FieldWithMetadata(conn, raw);
        Assert.assertEquals(/* remapped by TEXT special case */Types.VARCHAR, fieldWithMetadata.getJavaType());
        Assert.assertEquals("UTF-16", fieldWithMetadata.getEncoding());
        Assert.assertEquals(false, fieldWithMetadata.isSingleBit());

        conn.setIncludedFields(Query.ExecuteOptions.IncludedFields.TYPE_AND_NAME);
        fieldWithMetadata = new FieldWithMetadata(conn, raw);
        Assert.assertEquals(Types.VARBINARY, fieldWithMetadata.getJavaType());
        Assert.assertEquals(null, fieldWithMetadata.getEncoding());
        Assert.assertEquals(false, fieldWithMetadata.isSingleBit());

        conn = getVitessConnection();
        raw = raw.toBuilder()
            .setType(Query.Type.JSON)
            .setColumnLength(MysqlDefs.LENGTH_LONGBLOB)
            .setCharset(CharsetMapping.MYSQL_COLLATION_INDEX_binary)
            .build();

        fieldWithMetadata = new FieldWithMetadata(conn, raw);
        Assert.assertEquals(Types.CHAR, fieldWithMetadata.getJavaType());
        Assert.assertEquals("UTF-8", fieldWithMetadata.getEncoding());
        Assert.assertEquals(false, fieldWithMetadata.isSingleBit());

        conn.setIncludedFields(Query.ExecuteOptions.IncludedFields.TYPE_AND_NAME);
        fieldWithMetadata = new FieldWithMetadata(conn, raw);
        Assert.assertEquals(Types.CHAR, fieldWithMetadata.getJavaType());
        Assert.assertEquals(null, fieldWithMetadata.getEncoding());
        Assert.assertEquals(false, fieldWithMetadata.isSingleBit());

        conn = getVitessConnection();
        raw = raw.toBuilder()
            .setType(Query.Type.BIT)
            .build();
        fieldWithMetadata = new FieldWithMetadata(conn, raw);
        Assert.assertEquals(Types.BIT, fieldWithMetadata.getJavaType());
        Assert.assertEquals("ISO-8859-1", fieldWithMetadata.getEncoding());
        Assert.assertEquals(false, fieldWithMetadata.isSingleBit());
        Assert.assertEquals(false, fieldWithMetadata.isBlob());
        Assert.assertEquals(false, fieldWithMetadata.isBinary());

        conn.setIncludedFields(Query.ExecuteOptions.IncludedFields.TYPE_AND_NAME);
        fieldWithMetadata = new FieldWithMetadata(conn, raw);
        Assert.assertEquals(Types.BIT, fieldWithMetadata.getJavaType());
        Assert.assertEquals(null, fieldWithMetadata.getEncoding());
        Assert.assertEquals(false, fieldWithMetadata.isSingleBit());
        Assert.assertEquals(false, fieldWithMetadata.isBlob());
        Assert.assertEquals(false, fieldWithMetadata.isBinary());

        conn = getVitessConnection();
        raw = raw.toBuilder()
            .setColumnLength(1)
            .build();
        fieldWithMetadata = new FieldWithMetadata(conn, raw);
        Assert.assertEquals(Types.BIT, fieldWithMetadata.getJavaType());
        Assert.assertEquals("ISO-8859-1", fieldWithMetadata.getEncoding());
        Assert.assertEquals(true, fieldWithMetadata.isSingleBit());
        Assert.assertEquals(false, fieldWithMetadata.isBlob());
        Assert.assertEquals(false, fieldWithMetadata.isBinary());

        conn.setIncludedFields(Query.ExecuteOptions.IncludedFields.TYPE_AND_NAME);
        fieldWithMetadata = new FieldWithMetadata(conn, raw);
        Assert.assertEquals(Types.BIT, fieldWithMetadata.getJavaType());
        Assert.assertEquals(null, fieldWithMetadata.getEncoding());
        Assert.assertEquals(false, fieldWithMetadata.isSingleBit());
        Assert.assertEquals(false, fieldWithMetadata.isBlob());
        Assert.assertEquals(false, fieldWithMetadata.isBinary());
    }

    @Test
    public void testVarBinaryToVarCharRemapping() throws SQLException {
        VitessConnection conn = getVitessConnection();

        Query.Field raw = Query.Field.newBuilder()
            .setTable("foo")
            .setColumnLength(3)
            .setType(Query.Type.VARBINARY)
            .setName("foo")
            .setOrgName("foo")
            .setCharset(CharsetMapping.MYSQL_COLLATION_INDEX_binary)
            .setFlags(Query.MySqlFlag.BINARY_FLAG_VALUE)
            .build();

        FieldWithMetadata fieldWithMetadata = new FieldWithMetadata(conn, raw);
        Assert.assertEquals("no remapping - base case", Types.VARBINARY, fieldWithMetadata.getJavaType());

        raw = raw.toBuilder().setCharset(CharsetMapping.MYSQL_COLLATION_INDEX_utf8).build();
        fieldWithMetadata = new FieldWithMetadata(conn, raw);
        Assert.assertEquals("remap to varchar due to non-binary encoding", Types.VARCHAR, fieldWithMetadata.getJavaType());
    }

    @Test
    public void testBinaryToCharRemapping() throws SQLException {
        VitessConnection conn = getVitessConnection();

        Query.Field raw = Query.Field.newBuilder()
            .setTable("foo")
            .setColumnLength(3)
            .setType(Query.Type.BINARY)
            .setName("foo")
            .setOrgName("foo")
            .setCharset(CharsetMapping.MYSQL_COLLATION_INDEX_binary)
            .setFlags(Query.MySqlFlag.BINARY_FLAG_VALUE)
            .build();

        FieldWithMetadata fieldWithMetadata = new FieldWithMetadata(conn, raw);
        Assert.assertEquals("no remapping - base case", Types.BINARY, fieldWithMetadata.getJavaType());

        raw = raw.toBuilder().setCharset(CharsetMapping.MYSQL_COLLATION_INDEX_utf8).build();
        fieldWithMetadata = new FieldWithMetadata(conn, raw);
        Assert.assertEquals("remap to char due to non-binary encoding", Types.CHAR, fieldWithMetadata.getJavaType());
    }

    @Test
    public void testNumericAndDateTimeEncoding() throws SQLException{
        VitessConnection conn = getVitessConnection();

        Query.Type[] types = new Query.Type[]{
            Query.Type.INT8,
            Query.Type.UINT8,
            Query.Type.INT16,
            Query.Type.UINT16,
            Query.Type.INT24,
            Query.Type.UINT24,
            Query.Type.INT32,
            Query.Type.UINT32,
            Query.Type.INT64,
            Query.Type.UINT64,
            Query.Type.DECIMAL,
            Query.Type.UINT24,
            Query.Type.INT32,
            Query.Type.UINT32,
            Query.Type.FLOAT32,
            Query.Type.FLOAT64,
            Query.Type.DATE,
            Query.Type.DATETIME,
            Query.Type.TIME,
            Query.Type.TIMESTAMP,
            Query.Type.YEAR
        };


        for (Query.Type type : types) {
            Query.Field raw = Query.Field.newBuilder()
                .setTable("foo")
                .setColumnLength(3)
                .setType(type)
                .setName("foo")
                .setOrgName("foo")
                .setCharset(/* utf-16 UnicodeBig */35)
                .build();

            FieldWithMetadata fieldWithMetadata = new FieldWithMetadata(conn, raw);
            Assert.assertEquals(type.name(),"US-ASCII", fieldWithMetadata.getEncoding());
            Assert.assertEquals(type.name(),false, fieldWithMetadata.isSingleBit());
        }

        conn.setIncludedFields(Query.ExecuteOptions.IncludedFields.TYPE_AND_NAME);

        for (Query.Type type : types) {
            Query.Field raw = Query.Field.newBuilder()
                .setTable("foo")
                .setColumnLength(3)
                .setType(type)
                .setName("foo")
                .setOrgName("foo")
                .setCharset(/* utf-16 UnicodeBig */35)
                .build();

            FieldWithMetadata fieldWithMetadata = new FieldWithMetadata(conn, raw);
            Assert.assertEquals(type.name(),null, fieldWithMetadata.getEncoding());
            Assert.assertEquals(type.name(),false, fieldWithMetadata.isSingleBit());
        }
    }

    @Test
    public void testPrecisionAdjustFactor() throws SQLException {
        VitessConnection conn = getVitessConnection();
        assertPrecisionEquals(conn, Query.Type.FLOAT32, true, 0, 0);
        assertPrecisionEquals(conn, Query.Type.FLOAT64, true, 32, 0);
        assertPrecisionEquals(conn, Query.Type.BIT, true, 0, 0);
        assertPrecisionEquals(conn, Query.Type.DECIMAL, true, 0, -1);
        assertPrecisionEquals(conn, Query.Type.DECIMAL, true, 3, -2);
        assertPrecisionEquals(conn, Query.Type.INT32, true, /* this can't happen, but just checking */3, -2);
        assertPrecisionEquals(conn, Query.Type.INT32, true, 0, -1);
        assertPrecisionEquals(conn, Query.Type.FLOAT32, false, 0, 0);
        assertPrecisionEquals(conn, Query.Type.FLOAT64, false, 32, 0);
        assertPrecisionEquals(conn, Query.Type.BIT, false, 0, 0);
        assertPrecisionEquals(conn, Query.Type.DECIMAL, false, 0, -1);
        assertPrecisionEquals(conn, Query.Type.DECIMAL, false, 3, -1);
        assertPrecisionEquals(conn, Query.Type.UINT32, false, 0, 0);

        conn.setIncludedFields(Query.ExecuteOptions.IncludedFields.TYPE_AND_NAME);
        for (Query.Type type : Query.Type.values()) {
            if (type == Query.Type.UNRECOGNIZED || type == Query.Type.EXPRESSION) {
                continue;
            }

            // All should be 0
            assertPrecisionEquals(conn, type, true, 0, 0);
            assertPrecisionEquals(conn, type, false, 0, 0);
            assertPrecisionEquals(conn, type, true, 2, 0);
            assertPrecisionEquals(conn, type, false, 2, 0);
        }
    }

    private void assertPrecisionEquals(VitessConnection conn, Query.Type fieldType, boolean signed, int decimals, int expectedPrecisionAdjustFactor) throws SQLException {
        Query.Field raw = Query.Field.newBuilder()
            .setTable("foo")
            .setColumnLength(3)
            .setType(fieldType)
            .setDecimals(decimals)
            .setFlags(signed ? 0 : Query.MySqlFlag.UNSIGNED_FLAG_VALUE)
            .setName("foo")
            .setOrgName("foo")
            .build();
        FieldWithMetadata fieldWithMetadata = new FieldWithMetadata(conn, raw);
        Assert.assertEquals(expectedPrecisionAdjustFactor, fieldWithMetadata.getPrecisionAdjustFactor());
    }

    @Test
    public void testFlags() throws SQLException {
        VitessConnection conn = getVitessConnection();
        Query.Field raw = Query.Field.newBuilder()
            .setTable("foo")
            .setColumnLength(3)
            .setType(Query.Type.VARBINARY)
            .setName("foo")
            .setOrgName("foo")
            .build();
        FieldWithMetadata fieldWithMetadata = new FieldWithMetadata(conn, raw);
        Assert.assertEquals(false, fieldWithMetadata.isBinary());
        Assert.assertEquals(false, fieldWithMetadata.isBlob());
        Assert.assertEquals(false, fieldWithMetadata.isAutoIncrement());
        Assert.assertEquals(false, fieldWithMetadata.isMultipleKey());
        Assert.assertEquals(false, fieldWithMetadata.isNotNull());
        Assert.assertEquals(false, fieldWithMetadata.isPrimaryKey());
        Assert.assertEquals(false, fieldWithMetadata.isUniqueKey());
        Assert.assertEquals(false, fieldWithMetadata.isUnsigned());
        Assert.assertEquals(/* just inverses isUnsigned */true, fieldWithMetadata.isSigned());
        Assert.assertEquals(false, fieldWithMetadata.isZeroFill());

        int value = 0;
        for (Query.MySqlFlag flag : Query.MySqlFlag.values()) {
            if (flag == Query.MySqlFlag.UNRECOGNIZED) {
                continue;
            }
            value |= flag.getNumber();
        }
        raw = raw.toBuilder()
            .setFlags(value)
            .build();
        fieldWithMetadata = new FieldWithMetadata(conn, raw);
        Assert.assertEquals(true, fieldWithMetadata.isBinary());
        Assert.assertEquals(true, fieldWithMetadata.isBlob());
        Assert.assertEquals(true, fieldWithMetadata.isAutoIncrement());
        Assert.assertEquals(true, fieldWithMetadata.isMultipleKey());
        Assert.assertEquals(true, fieldWithMetadata.isNotNull());
        Assert.assertEquals(true, fieldWithMetadata.isPrimaryKey());
        Assert.assertEquals(true, fieldWithMetadata.isUniqueKey());
        Assert.assertEquals(true, fieldWithMetadata.isUnsigned());
        Assert.assertEquals(/* just inverses isUnsigned */false, fieldWithMetadata.isSigned());
        Assert.assertEquals(true, fieldWithMetadata.isZeroFill());

        conn.setIncludedFields(Query.ExecuteOptions.IncludedFields.TYPE_AND_NAME);
        fieldWithMetadata = new FieldWithMetadata(conn, raw);
        Assert.assertEquals(false, fieldWithMetadata.isBinary());
        Assert.assertEquals(false, fieldWithMetadata.isBlob());
        Assert.assertEquals(false, fieldWithMetadata.isAutoIncrement());
        Assert.assertEquals(false, fieldWithMetadata.isMultipleKey());
        Assert.assertEquals(true, fieldWithMetadata.isNotNull());
        Assert.assertEquals(false, fieldWithMetadata.isPrimaryKey());
        Assert.assertEquals(false, fieldWithMetadata.isUniqueKey());
        Assert.assertEquals(true, fieldWithMetadata.isUnsigned());
        Assert.assertEquals(/* just inverses isUnsigned */false, fieldWithMetadata.isSigned());
        Assert.assertEquals(false, fieldWithMetadata.isZeroFill());

    }

    @Test
    public void testOpaqueBinary() throws SQLException {
        VitessConnection conn = getVitessConnection();

        Query.Field raw = Query.Field.newBuilder()
            .setTable("foo")
            .setColumnLength(3)
            .setType(Query.Type.CHAR)
            .setName("foo")
            .setOrgName("foo")
            .setCharset(CharsetMapping.MYSQL_COLLATION_INDEX_binary)
            .setFlags(Query.MySqlFlag.BINARY_FLAG_VALUE)
            .build();

        FieldWithMetadata fieldWithMetadata = new FieldWithMetadata(conn, raw);
        Assert.assertEquals(true, fieldWithMetadata.isOpaqueBinary());

        raw = raw.toBuilder()
            .setTable("#sql_foo_bar")
            .build();
        fieldWithMetadata = new FieldWithMetadata(conn, raw);
        Assert.assertEquals(false, fieldWithMetadata.isOpaqueBinary());

        raw = raw.toBuilder()
            .setCharset(/* short circuits collation -> encoding lookup, resulting in null */-1)
            .build();

        fieldWithMetadata = new FieldWithMetadata(conn, raw);
        Assert.assertEquals(false, fieldWithMetadata.isOpaqueBinary());

        conn.setEncoding("binary");
        fieldWithMetadata = new FieldWithMetadata(conn, raw);
        Assert.assertEquals(true, fieldWithMetadata.isOpaqueBinary());

        conn.setIncludedFields(Query.ExecuteOptions.IncludedFields.TYPE_AND_NAME);
        fieldWithMetadata = new FieldWithMetadata(conn, raw);
        Assert.assertEquals(false, fieldWithMetadata.isOpaqueBinary());
    }

    @Test
    public void testReadOnly() throws SQLException {
        VitessConnection conn = getVitessConnection();
        Query.Field raw = Query.Field.newBuilder()
            .setTable("foo")
            .setType(Query.Type.CHAR)
            .setName("foo")
            .setOrgName("foo")
            .setOrgTable("foo")
            .build();
        FieldWithMetadata fieldWithMetadata = new FieldWithMetadata(conn, raw);
        Assert.assertEquals(false, fieldWithMetadata.isReadOnly());

        raw = raw.toBuilder()
            .setOrgName("")
            .setOrgTable("foo")
            .build();
        fieldWithMetadata = new FieldWithMetadata(conn, raw);
        Assert.assertEquals(true, fieldWithMetadata.isReadOnly());

        raw = raw.toBuilder()
            .setOrgName("foo")
            .setOrgTable("")
            .build();
        fieldWithMetadata = new FieldWithMetadata(conn, raw);
        Assert.assertEquals(true, fieldWithMetadata.isReadOnly());

        raw = raw.toBuilder()
            .setOrgTable("")
            .setOrgName("")
            .build();
        fieldWithMetadata = new FieldWithMetadata(conn, raw);
        Assert.assertEquals(true, fieldWithMetadata.isReadOnly());

        conn.setIncludedFields(Query.ExecuteOptions.IncludedFields.TYPE_AND_NAME);
        fieldWithMetadata = new FieldWithMetadata(conn, raw);
        Assert.assertEquals(false, fieldWithMetadata.isReadOnly());
    }

    @Test
    public void testDefaultsWithoutAllFields() throws SQLException {
        Query.Field raw = Query.Field.newBuilder()
            .setName("foo")
            .setOrgName("foo")
            .setTable("foo")
            .setOrgTable("foo")
            .setDatabase("foo")
            .setType(Query.Type.CHAR)
            .setFlags(Query.MySqlFlag.AUTO_INCREMENT_FLAG_VALUE |
                Query.MySqlFlag.PRI_KEY_FLAG_VALUE |
                Query.MySqlFlag.UNIQUE_KEY_FLAG_VALUE |
                Query.MySqlFlag.BINARY_FLAG_VALUE |
                Query.MySqlFlag.BLOB_FLAG_VALUE |
                Query.MySqlFlag.MULTIPLE_KEY_FLAG_VALUE |
                Query.MySqlFlag.UNSIGNED_FLAG_VALUE |
                Query.MySqlFlag.ZEROFILL_FLAG_VALUE
            )
            .build();
        VitessConnection conn = getVitessConnection();
        conn.setIncludedFields(Query.ExecuteOptions.IncludedFields.TYPE_AND_NAME);
        FieldWithMetadata field = new FieldWithMetadata(conn, raw);
        Assert.assertEquals("foo", field.getName());
        Assert.assertEquals(null, field.getOrgName());
        Assert.assertEquals(null, field.getTable());
        Assert.assertEquals(null, field.getOrgTable());
        Assert.assertEquals(null, field.getDatabase());
        Assert.assertEquals(0, field.getDecimals());
        Assert.assertEquals(0, field.getColumnLength());
        Assert.assertEquals(0, field.getPrecisionAdjustFactor());
        Assert.assertEquals(false, field.isSingleBit());
        Assert.assertEquals(false, field.isAutoIncrement());
        Assert.assertEquals(false, field.isBinary());
        Assert.assertEquals(false, field.isBlob());
        Assert.assertEquals(false, field.isMultipleKey());
        Assert.assertEquals(true, field.isNotNull());
        Assert.assertEquals(false, field.isZeroFill());
        Assert.assertEquals(false, field.isPrimaryKey());
        Assert.assertEquals(false, field.isUniqueKey());
        Assert.assertEquals(true, field.isUnsigned());
        Assert.assertEquals(false, field.isSigned());
        Assert.assertEquals(false, field.isOpaqueBinary());
        Assert.assertEquals(false, field.isReadOnly());
    }

    @Test
    public void testToString() throws SQLException {
        Query.Field raw = Query.Field.newBuilder()
            .setName("foo")
            .setOrgName("foo")
            .setTable("foo")
            .setOrgTable("foo")
            .setDatabase("foo")
            .setType(Query.Type.CHAR)
            .setFlags(Query.MySqlFlag.AUTO_INCREMENT_FLAG_VALUE |
                    Query.MySqlFlag.PRI_KEY_FLAG_VALUE |
                    Query.MySqlFlag.UNIQUE_KEY_FLAG_VALUE |
                    Query.MySqlFlag.BINARY_FLAG_VALUE |
                    Query.MySqlFlag.BLOB_FLAG_VALUE |
                    Query.MySqlFlag.MULTIPLE_KEY_FLAG_VALUE |
                    Query.MySqlFlag.UNSIGNED_FLAG_VALUE |
                    Query.MySqlFlag.ZEROFILL_FLAG_VALUE
            )
            .build();
        FieldWithMetadata field = new FieldWithMetadata(getVitessConnection(), raw);
        String result = "io.vitess.jdbc.FieldWithMetadata[catalog=foo," +
            "tableName=foo,originalTableName=foo," +
            "columnName=foo,originalColumnName=foo," +
            "vitessType=" + Query.Type.CHAR.toString() + "(1)," +
            "flags=AUTO_INCREMENT PRIMARY_KEY UNIQUE_KEY BINARY " +
            "BLOB MULTI_KEY UNSIGNED ZEROFILL, charsetIndex=0, charsetName=null]";
        Assert.assertEquals(result, field.toString());
    }

    public void testCollations() throws Exception {
        VitessConnection conn = getVitessConnection();

        Query.Field raw = Query.Field.newBuilder()
            .setTable("foo")
            .setType(Query.Type.CHAR)
            .setName("foo")
            .setOrgName("foo")
            .setCharset(33)
            .build();

        FieldWithMetadata fieldWithMetadata = PowerMockito.spy(new FieldWithMetadata(conn, raw));
        String first = fieldWithMetadata.getCollation();
        String second = fieldWithMetadata.getCollation();

        Assert.assertEquals("utf8_general_ci", first);
        Assert.assertEquals("cached response is same as first", first, second);

        PowerMockito.verifyPrivate(fieldWithMetadata, VerificationModeFactory.times(1)).invoke("getCollationIndex");

        try {
            raw = raw.toBuilder()
                // value chosen because it's obviously out of bounds for the underlying array
                .setCharset(Integer.MAX_VALUE)
                .build();

            fieldWithMetadata = PowerMockito.spy(new FieldWithMetadata(conn, raw));
            fieldWithMetadata.getCollation();
            Assert.fail("Should have received an array index out of bounds because " +
                "charset/collationIndex of Int.MAX is well above size of charset array");
        } catch (SQLException e) {
            if (e.getCause() instanceof ArrayIndexOutOfBoundsException) {
                Assert.assertEquals("CollationIndex '" + Integer.MAX_VALUE + "' out of bounds for " +
                    "collationName lookup, should be within 0 and " +
                    CharsetMapping.COLLATION_INDEX_TO_COLLATION_NAME.length,
                    e.getMessage());
            } else {
                // just rethrow so we fail that way
                throw e;
            }
        }

        PowerMockito.verifyPrivate(fieldWithMetadata, VerificationModeFactory.times(1)).invoke("getCollationIndex");
        //Mockito.verify(fieldWithMetadata, Mockito.times(1)).getCollationIndex();

        conn.setIncludedFields(Query.ExecuteOptions.IncludedFields.TYPE_AND_NAME);
        fieldWithMetadata = PowerMockito.spy(new FieldWithMetadata(conn, raw));
        Assert.assertEquals("null response when not including all fields", null, fieldWithMetadata.getCollation());

        // We should not call this at all, because we're short circuiting due to included fields
        //Mockito.verify(fieldWithMetadata, Mockito.never()).getCollationIndex();
        PowerMockito.verifyPrivate(fieldWithMetadata, VerificationModeFactory.times(0)).invoke("getCollationIndex");
    }

    @Test
    public void testMaxBytesPerChar() throws Exception {
        VitessConnection conn = PowerMockito.spy(getVitessConnection());

        Query.Field raw = Query.Field.newBuilder()
            .setTable("foo")
            .setType(Query.Type.CHAR)
            .setName("foo")
            .setOrgName("foo")
            .setCharset(33)
            .build();

        FieldWithMetadata fieldWithMetadata = PowerMockito.spy(new FieldWithMetadata(conn, raw));

        int first = fieldWithMetadata.getMaxBytesPerCharacter();
        int second = fieldWithMetadata.getMaxBytesPerCharacter();

        Assert.assertEquals("cached response is same as first", first, second);
        // We called getMaxBytesPerCharacter 2 times above, but should only have made 1 call to fieldWithMetadata.getMaxBytesPerChar:
        // first - call conn
        // second - return cached
        Mockito.verify(fieldWithMetadata, VerificationModeFactory.times(1)).getMaxBytesPerChar(33, "UTF-8");
        PowerMockito.verifyPrivate(fieldWithMetadata, VerificationModeFactory.times(1)).invoke("getCollationIndex");

        conn.setIncludedFields(Query.ExecuteOptions.IncludedFields.TYPE_AND_NAME);
        fieldWithMetadata = PowerMockito.spy(new FieldWithMetadata(conn, raw));
        Assert.assertEquals("0 return value when not including all fields", 0, fieldWithMetadata.getMaxBytesPerCharacter());

        // We should not call this function because we short circuited due to not including all fields.
        Mockito.verify(fieldWithMetadata, VerificationModeFactory.times(0)).getMaxBytesPerChar(33, "UTF-8");
        // Should not be called at all, because it's new for just this test
        PowerMockito.verifyPrivate(fieldWithMetadata, VerificationModeFactory.times(0)).invoke("getCollationIndex");
    }

    @Test public void testGetEncodingForIndex() throws SQLException {
        Query.Field raw = Query.Field.newBuilder()
          .setTable("foo")
          .setType(Query.Type.CHAR)
          .setName("foo")
          .setOrgName("foo")
          .setCharset(33)
          .build();
        FieldWithMetadata field = new FieldWithMetadata(getVitessConnection(), raw);

        // No default encoding configured, and passing NO_CHARSET_INFO basically says "mysql doesn't know"
        // which means don't try looking it up
        Assert.assertEquals(null, field.getEncodingForIndex(MysqlDefs.NO_CHARSET_INFO));
        // Similarly, a null index or one landing out of bounds for the charset index should return null
        Assert.assertEquals(null, field.getEncodingForIndex(Integer.MAX_VALUE));
        Assert.assertEquals(null, field.getEncodingForIndex(-123));

        // charsetIndex 25 is MYSQL_CHARSET_NAME_greek, which is a charset with multiple names, ISO8859_7 and greek
        // Without an encoding configured in the connection, we should return the first (default) encoding for a charset,
        // in this case ISO8859_7
        Assert.assertEquals("ISO-8859-7", field.getEncodingForIndex(25));
        field.getConnectionProperties().setEncoding("greek");
        // With an encoding configured, we should return that because it matches one of the names for the charset
        Assert.assertEquals("greek", field.getEncodingForIndex(25));

        field.getConnectionProperties().setEncoding(null);
        Assert.assertEquals("UTF-8", field.getEncodingForIndex(CharsetMapping.MYSQL_COLLATION_INDEX_utf8));
        Assert.assertEquals("ISO-8859-1", field.getEncodingForIndex(CharsetMapping.MYSQL_COLLATION_INDEX_binary));

        field.getConnectionProperties().setEncoding("NOT_REAL");
        // Same tests as the first one, but testing that when there is a default configured, it falls back to that regardless
        Assert.assertEquals("NOT_REAL", field.getEncodingForIndex(MysqlDefs.NO_CHARSET_INFO));
        Assert.assertEquals("NOT_REAL", field.getEncodingForIndex(Integer.MAX_VALUE));
        Assert.assertEquals("NOT_REAL", field.getEncodingForIndex(-123));
    }

    @Test public void testGetMaxBytesPerChar() throws SQLException {
        Query.Field raw = Query.Field.newBuilder()
          .setTable("foo")
          .setType(Query.Type.CHAR)
          .setName("foo")
          .setOrgName("foo")
          .setCharset(33)
          .build();
        FieldWithMetadata field = new FieldWithMetadata(getVitessConnection(), raw);

        // Default state when no good info is passed in
        Assert.assertEquals(0, field.getMaxBytesPerChar(MysqlDefs.NO_CHARSET_INFO, null));
        // use passed collation index
        Assert.assertEquals(3, field.getMaxBytesPerChar(CharsetMapping.MYSQL_COLLATION_INDEX_utf8, null));
        // use first, if both are passed and valid
        Assert.assertEquals(3, field.getMaxBytesPerChar(CharsetMapping.MYSQL_COLLATION_INDEX_utf8, "UnicodeBig"));
        // use passed default charset
        Assert.assertEquals(2, field.getMaxBytesPerChar(MysqlDefs.NO_CHARSET_INFO, "UnicodeBig"));
    }
}
