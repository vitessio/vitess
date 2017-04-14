package io.vitess.jdbc;

import com.google.protobuf.ByteString;
import io.vitess.client.cursor.Cursor;
import io.vitess.client.cursor.SimpleCursor;
import io.vitess.proto.Query;
import io.vitess.util.MysqlDefs;
import io.vitess.util.StringUtils;
import io.vitess.util.charset.CharsetMapping;
import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Matchers;
import org.mockito.internal.verification.VerificationModeFactory;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

/**
 * Created by harshit.gangal on 19/01/16.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(VitessResultSet.class)
public class VitessResultSetTest extends BaseTest {

    public Cursor getCursorWithRows() {
        /*
        INT8(1, 257), -50
        UINT8(2, 770), 50
        INT16(3, 259), -23000
        UINT16(4, 772), 23000
        INT24(5, 261), -100
        UINT24(6, 774), 100
        INT32(7, 263), -100
        UINT32(8, 776), 100
        INT64(9, 265),  -1000
        UINT64(10, 778), 1000
        FLOAT32(11, 1035), 24.53
        FLOAT64(12, 1036), 100.43
        TIMESTAMP(13, 2061), 2016-02-06 14:15:16
        DATE(14, 2062), 2016-02-06
        TIME(15, 2063), 12:34:56
        DATETIME(16, 2064), 2016-02-06 14:15:16
        YEAR(17, 785),  2016
        DECIMAL(18, 18), 1234.56789
        TEXT(19, 6163), HELLO TDS TEAM
        BLOB(20, 10260),  HELLO TDS TEAM
        VARCHAR(21, 6165), HELLO TDS TEAM
        VARBINARY(22, 10262), HELLO TDS TEAM
        CHAR(23, 6167), N
        BINARY(24, 10264), HELLO TDS TEAM
        BIT(25, 2073), 1
        ENUM(26, 2074), val123
        SET(27, 2075), val123
        TUPLE(28, 28),
        UNRECOGNIZED(-1, -1);
        */
        return new SimpleCursor(Query.QueryResult.newBuilder()
            .addFields(Query.Field.newBuilder().setName("col1").setType(Query.Type.INT8).build())
            .addFields(Query.Field.newBuilder().setName("col2").setType(Query.Type.UINT8).build())
            .addFields(Query.Field.newBuilder().setName("col3").setType(Query.Type.INT16).build())
            .addFields(Query.Field.newBuilder().setName("col4").setType(Query.Type.UINT16).build())
            .addFields(Query.Field.newBuilder().setName("col5").setType(Query.Type.INT24).build())
            .addFields(Query.Field.newBuilder().setName("col6").setType(Query.Type.UINT24).build())
            .addFields(Query.Field.newBuilder().setName("col7").setType(Query.Type.INT32).build())
            .addFields(Query.Field.newBuilder().setName("col8").setType(Query.Type.UINT32).build())
            .addFields(Query.Field.newBuilder().setName("col9").setType(Query.Type.INT64).build())
            .addFields(Query.Field.newBuilder().setName("col10").setType(Query.Type.UINT64).build())
            .addFields(
                Query.Field.newBuilder().setName("col11").setType(Query.Type.FLOAT32).build())
            .addFields(
                Query.Field.newBuilder().setName("col12").setType(Query.Type.FLOAT64).build())
            .addFields(
                Query.Field.newBuilder().setName("col13").setType(Query.Type.TIMESTAMP).build())
            .addFields(Query.Field.newBuilder().setName("col14").setType(Query.Type.DATE).build())
            .addFields(Query.Field.newBuilder().setName("col15").setType(Query.Type.TIME).build())
            .addFields(
                Query.Field.newBuilder().setName("col16").setType(Query.Type.DATETIME).build())
            .addFields(Query.Field.newBuilder().setName("col17").setType(Query.Type.YEAR).build())
            .addFields(
                Query.Field.newBuilder().setName("col18").setType(Query.Type.DECIMAL).build())
            .addFields(Query.Field.newBuilder().setName("col19").setType(Query.Type.TEXT).build())
            .addFields(Query.Field.newBuilder().setName("col20").setType(Query.Type.BLOB).build())
            .addFields(
                Query.Field.newBuilder().setName("col21").setType(Query.Type.VARCHAR).build())
            .addFields(
                Query.Field.newBuilder().setName("col22").setType(Query.Type.VARBINARY).build())
            .addFields(Query.Field.newBuilder().setName("col23").setType(Query.Type.CHAR).build())
            .addFields(Query.Field.newBuilder().setName("col24").setType(Query.Type.BINARY).build())
            .addFields(Query.Field.newBuilder().setName("col25").setType(Query.Type.BIT).build())
            .addFields(Query.Field.newBuilder().setName("col26").setType(Query.Type.ENUM).build())
            .addFields(Query.Field.newBuilder().setName("col27").setType(Query.Type.SET).build())
            .addRows(Query.Row.newBuilder().addLengths("-50".length()).addLengths("50".length())
                .addLengths("-23000".length()).addLengths("23000".length())
                .addLengths("-100".length()).addLengths("100".length()).addLengths("-100".length())
                .addLengths("100".length()).addLengths("-1000".length()).addLengths("1000".length())
                .addLengths("24.52".length()).addLengths("100.43".length())
                .addLengths("2016-02-06 14:15:16".length()).addLengths("2016-02-06".length())
                .addLengths("12:34:56".length()).addLengths("2016-02-06 14:15:16".length())
                .addLengths("2016".length()).addLengths("1234.56789".length())
                .addLengths("HELLO TDS TEAM".length()).addLengths("HELLO TDS TEAM".length())
                .addLengths("HELLO TDS TEAM".length()).addLengths("HELLO TDS TEAM".length())
                .addLengths("N".length()).addLengths("HELLO TDS TEAM".length())
                .addLengths("1".length()).addLengths("val123".length())
                .addLengths("val123".length()).setValues(ByteString
                    .copyFromUtf8("-5050-2300023000-100100-100100-1000100024.52100.432016-02-06 " +
                        "14:15:162016-02-0612:34:562016-02-06 14:15:1620161234.56789HELLO TDS TEAMHELLO TDS TEAMHELLO"
                        +
                        " TDS TEAMHELLO TDS TEAMNHELLO TDS TEAM1val123val123"))).build());
    }

    public Cursor getCursorWithRowsAsNull() {
        /*
        INT8(1, 257), -50
        UINT8(2, 770), 50
        INT16(3, 259), -23000
        UINT16(4, 772), 23000
        INT24(5, 261), -100
        UINT24(6, 774), 100
        INT32(7, 263), -100
        UINT32(8, 776), 100
        INT64(9, 265),  -1000
        UINT64(10, 778), 1000
        FLOAT32(11, 1035), 24.53
        FLOAT64(12, 1036), 100.43
        TIMESTAMP(13, 2061), 2016-02-06 14:15:16
        DATE(14, 2062), 2016-02-06
        TIME(15, 2063), 12:34:56
        DATETIME(16, 2064), 2016-02-06 14:15:16
        YEAR(17, 785),  2016
        DECIMAL(18, 18), 1234.56789
        TEXT(19, 6163), HELLO TDS TEAM
        BLOB(20, 10260),  HELLO TDS TEAM
        VARCHAR(21, 6165), HELLO TDS TEAM
        VARBINARY(22, 10262), HELLO TDS TEAM
        CHAR(23, 6167), N
        BINARY(24, 10264), HELLO TDS TEAM
        BIT(25, 2073), 0
        ENUM(26, 2074), val123
        SET(27, 2075), val123
        TUPLE(28, 28),
        UNRECOGNIZED(-1, -1);
        */
        return new SimpleCursor(Query.QueryResult.newBuilder()
            .addFields(Query.Field.newBuilder().setName("col1").setType(Query.Type.INT8).build())
            .addFields(Query.Field.newBuilder().setName("col2").setType(Query.Type.UINT8).build())
            .addFields(Query.Field.newBuilder().setName("col3").setType(Query.Type.INT16).build())
            .addFields(Query.Field.newBuilder().setName("col4").setType(Query.Type.UINT16).build())
            .addFields(Query.Field.newBuilder().setName("col5").setType(Query.Type.INT24).build())
            .addFields(Query.Field.newBuilder().setName("col6").setType(Query.Type.UINT24).build())
            .addFields(Query.Field.newBuilder().setName("col7").setType(Query.Type.INT32).build())
            .addFields(Query.Field.newBuilder().setName("col8").setType(Query.Type.UINT32).build())
            .addFields(Query.Field.newBuilder().setName("col9").setType(Query.Type.INT64).build())
            .addFields(Query.Field.newBuilder().setName("col10").setType(Query.Type.UINT64).build())
            .addFields(
                Query.Field.newBuilder().setName("col11").setType(Query.Type.FLOAT32).build())
            .addFields(
                Query.Field.newBuilder().setName("col12").setType(Query.Type.FLOAT64).build())
            .addFields(
                Query.Field.newBuilder().setName("col13").setType(Query.Type.TIMESTAMP).build())
            .addFields(Query.Field.newBuilder().setName("col14").setType(Query.Type.DATE).build())
            .addFields(Query.Field.newBuilder().setName("col15").setType(Query.Type.TIME).build())
            .addFields(
                Query.Field.newBuilder().setName("col16").setType(Query.Type.DATETIME).build())
            .addFields(Query.Field.newBuilder().setName("col17").setType(Query.Type.YEAR).build())
            .addFields(
                Query.Field.newBuilder().setName("col18").setType(Query.Type.DECIMAL).build())
            .addFields(Query.Field.newBuilder().setName("col19").setType(Query.Type.TEXT).build())
            .addFields(Query.Field.newBuilder().setName("col20").setType(Query.Type.BLOB).build())
            .addFields(
                Query.Field.newBuilder().setName("col21").setType(Query.Type.VARCHAR).build())
            .addFields(
                Query.Field.newBuilder().setName("col22").setType(Query.Type.VARBINARY).build())
            .addFields(Query.Field.newBuilder().setName("col23").setType(Query.Type.CHAR).build())
            .addFields(Query.Field.newBuilder().setName("col24").setType(Query.Type.BINARY).build())
            .addFields(Query.Field.newBuilder().setName("col25").setType(Query.Type.BIT).build())
            .addFields(Query.Field.newBuilder().setName("col26").setType(Query.Type.ENUM).build())
            .addFields(Query.Field.newBuilder().setName("col27").setType(Query.Type.SET).build())
            .addRows(Query.Row.newBuilder().addLengths("-50".length()).addLengths("50".length())
                .addLengths("-23000".length()).addLengths("23000".length())
                .addLengths("-100".length()).addLengths("100".length()).addLengths("-100".length())
                .addLengths("100".length()).addLengths("-1000".length()).addLengths("1000".length())
                .addLengths("24.52".length()).addLengths("100.43".length())
                .addLengths("2016-02-06 14:15:16".length()).addLengths("2016-02-06".length())
                .addLengths("12:34:56".length()).addLengths("2016-02-06 14:15:16".length())
                .addLengths("2016".length()).addLengths("1234.56789".length())
                .addLengths("HELLO TDS TEAM".length()).addLengths("HELLO TDS TEAM".length())
                .addLengths("HELLO TDS TEAM".length()).addLengths("HELLO TDS TEAM".length())
                .addLengths("N".length()).addLengths("HELLO TDS TEAM".length())
                .addLengths("0".length()).addLengths("val123".length()).addLengths(-1).setValues(
                    ByteString.copyFromUtf8(
                        "-5050-2300023000-100100-100100-1000100024.52100.432016-02-06 " +
                            "14:15:162016-02-0612:34:562016-02-06 14:15:1620161234.56789HELLO TDS TEAMHELLO TDS "
                            +
                            "TEAMHELLO TDS TEAMHELLO TDS TEAMNHELLO TDS TEAM0val123"))).build());
    }


    @Test public void testNextWithZeroRows() throws Exception {
        Cursor cursor = new SimpleCursor(Query.QueryResult.newBuilder()
            .addFields(Query.Field.newBuilder().setName("col0").build())
            .addFields(Query.Field.newBuilder().setName("col1").build())
            .addFields(Query.Field.newBuilder().setName("col2").build()).build());

        VitessResultSet vitessResultSet = new VitessResultSet(cursor);
        Assert.assertEquals(false, vitessResultSet.next());
    }

    @Test public void testNextWithNonZeroRows() throws Exception {
        Cursor cursor = getCursorWithRows();
        VitessResultSet vitessResultSet = new VitessResultSet(cursor);
        Assert.assertEquals(true, vitessResultSet.next());
        Assert.assertEquals(false, vitessResultSet.next());
    }

    @Test public void testgetString() throws SQLException {
        Cursor cursor = getCursorWithRowsAsNull();
        VitessResultSet vitessResultSet = new VitessResultSet(cursor, getVitessStatement());
        vitessResultSet.next();
        Assert.assertEquals("-50", vitessResultSet.getString(1));
        Assert.assertEquals("50", vitessResultSet.getString(2));
        Assert.assertEquals("-23000", vitessResultSet.getString(3));
        Assert.assertEquals("23000", vitessResultSet.getString(4));
        Assert.assertEquals("-100", vitessResultSet.getString(5));
        Assert.assertEquals("100", vitessResultSet.getString(6));
        Assert.assertEquals("-100", vitessResultSet.getString(7));
        Assert.assertEquals("100", vitessResultSet.getString(8));
        Assert.assertEquals("-1000", vitessResultSet.getString(9));
        Assert.assertEquals("1000", vitessResultSet.getString(10));
        Assert.assertEquals("24.52", vitessResultSet.getString(11));
        Assert.assertEquals("100.43", vitessResultSet.getString(12));
        Assert.assertEquals("2016-02-06 14:15:16.0", vitessResultSet.getString(13));
        Assert.assertEquals("2016-02-06", vitessResultSet.getString(14));
        Assert.assertEquals("12:34:56", vitessResultSet.getString(15));
        Assert.assertEquals("2016-02-06 14:15:16.0", vitessResultSet.getString(16));
        Assert.assertEquals("2016", vitessResultSet.getString(17));
        Assert.assertEquals("1234.56789", vitessResultSet.getString(18));
        Assert.assertEquals("HELLO TDS TEAM", vitessResultSet.getString(19));
        Assert.assertEquals("HELLO TDS TEAM", vitessResultSet.getString(20));
        Assert.assertEquals("HELLO TDS TEAM", vitessResultSet.getString(21));
        Assert.assertEquals("HELLO TDS TEAM", vitessResultSet.getString(22));
        Assert.assertEquals("N", vitessResultSet.getString(23));
        Assert.assertEquals("HELLO TDS TEAM", vitessResultSet.getString(24));
        Assert.assertEquals("0", vitessResultSet.getString(25));
        Assert.assertEquals("val123", vitessResultSet.getString(26));
        Assert.assertEquals(null, vitessResultSet.getString(27));
    }

    @Test public void testgetBoolean() throws SQLException {
        Cursor cursor = getCursorWithRows();
        Cursor cursorWithRowsAsNull = getCursorWithRowsAsNull();
        VitessResultSet vitessResultSet = new VitessResultSet(cursor, getVitessStatement());
        vitessResultSet.next();
        Assert.assertEquals(true, vitessResultSet.getBoolean(25));
        Assert.assertEquals(false, vitessResultSet.getBoolean(1));
        vitessResultSet = new VitessResultSet(cursorWithRowsAsNull, getVitessStatement());
        vitessResultSet.next();
        Assert.assertEquals(false, vitessResultSet.getBoolean(25));
        Assert.assertEquals(false, vitessResultSet.getBoolean(1));
    }

    @Test public void testgetByte() throws SQLException {
        Cursor cursor = getCursorWithRows();
        VitessResultSet vitessResultSet = new VitessResultSet(cursor, getVitessStatement());
        vitessResultSet.next();
        Assert.assertEquals(-50, vitessResultSet.getByte(1));
        Assert.assertEquals(1, vitessResultSet.getByte(25));
    }

    @Test public void testgetShort() throws SQLException {
        Cursor cursor = getCursorWithRows();
        VitessResultSet vitessResultSet = new VitessResultSet(cursor, getVitessStatement());
        vitessResultSet.next();
        Assert.assertEquals(-23000, vitessResultSet.getShort(3));
    }

    @Test public void testgetInt() throws SQLException {
        Cursor cursor = getCursorWithRows();
        VitessResultSet vitessResultSet = new VitessResultSet(cursor, getVitessStatement());
        vitessResultSet.next();
        Assert.assertEquals(-100, vitessResultSet.getInt(7));
    }

    @Test public void testgetLong() throws SQLException {
        Cursor cursor = getCursorWithRows();
        VitessResultSet vitessResultSet = new VitessResultSet(cursor, getVitessStatement());
        vitessResultSet.next();
        Assert.assertEquals(-1000, vitessResultSet.getInt(9));
    }

    @Test public void testgetFloat() throws SQLException {
        Cursor cursor = getCursorWithRows();
        VitessResultSet vitessResultSet = new VitessResultSet(cursor, getVitessStatement());
        vitessResultSet.next();
        Assert.assertEquals(24.52f, vitessResultSet.getFloat(11), 0.001);
    }

    @Test public void testgetDouble() throws SQLException {
        Cursor cursor = getCursorWithRows();
        VitessResultSet vitessResultSet = new VitessResultSet(cursor, getVitessStatement());
        vitessResultSet.next();
        Assert.assertEquals(100.43, vitessResultSet.getFloat(12), 0.001);
    }

    @Test public void testBigDecimal() throws SQLException {
        Cursor cursor = getCursorWithRows();
        VitessResultSet vitessResultSet = new VitessResultSet(cursor, getVitessStatement());
        vitessResultSet.next();
        Assert.assertEquals(new BigDecimal(BigInteger.valueOf(123456789), 5),
            vitessResultSet.getBigDecimal(18));
    }

    @Test public void testgetBytes() throws SQLException, UnsupportedEncodingException {
        Cursor cursor = getCursorWithRows();
        VitessResultSet vitessResultSet = new VitessResultSet(cursor, getVitessStatement());
        vitessResultSet.next();
        Assert.assertArrayEquals("HELLO TDS TEAM".getBytes("UTF-8"), vitessResultSet.getBytes(19));
    }

    @Test public void testgetDate() throws SQLException, UnsupportedEncodingException {
        Cursor cursor = getCursorWithRows();
        VitessResultSet vitessResultSet = new VitessResultSet(cursor, getVitessStatement());
        vitessResultSet.next();
        Assert.assertEquals(new java.sql.Date(116, 1, 6), vitessResultSet.getDate(14));
    }

    @Test public void testgetTime() throws SQLException, UnsupportedEncodingException {
        Cursor cursor = getCursorWithRows();
        VitessResultSet vitessResultSet = new VitessResultSet(cursor, getVitessStatement());
        vitessResultSet.next();
        Assert.assertEquals(new Time(12, 34, 56), vitessResultSet.getTime(15));
    }

    @Test public void testgetTimestamp() throws SQLException, UnsupportedEncodingException {
        Cursor cursor = getCursorWithRows();
        VitessResultSet vitessResultSet = new VitessResultSet(cursor, getVitessStatement());
        vitessResultSet.next();
        Assert.assertEquals(new Timestamp(116, 1, 6, 14, 15, 16, 0),
            vitessResultSet.getTimestamp(13));
    }

    @Test public void testgetStringbyColumnLabel() throws SQLException {
        Cursor cursor = getCursorWithRows();
        VitessResultSet vitessResultSet = new VitessResultSet(cursor, getVitessStatement());
        vitessResultSet.next();
        Assert.assertEquals("-50", vitessResultSet.getString("col1"));
        Assert.assertEquals("50", vitessResultSet.getString("col2"));
        Assert.assertEquals("-23000", vitessResultSet.getString("col3"));
        Assert.assertEquals("23000", vitessResultSet.getString("col4"));
        Assert.assertEquals("-100", vitessResultSet.getString("col5"));
        Assert.assertEquals("100", vitessResultSet.getString("col6"));
        Assert.assertEquals("-100", vitessResultSet.getString("col7"));
        Assert.assertEquals("100", vitessResultSet.getString("col8"));
        Assert.assertEquals("-1000", vitessResultSet.getString("col9"));
        Assert.assertEquals("1000", vitessResultSet.getString("col10"));
        Assert.assertEquals("24.52", vitessResultSet.getString("col11"));
        Assert.assertEquals("100.43", vitessResultSet.getString("col12"));
        Assert.assertEquals("2016-02-06 14:15:16.0", vitessResultSet.getString("col13"));
        Assert.assertEquals("2016-02-06", vitessResultSet.getString("col14"));
        Assert.assertEquals("12:34:56", vitessResultSet.getString("col15"));
        Assert.assertEquals("2016-02-06 14:15:16.0", vitessResultSet.getString("col16"));
        Assert.assertEquals("2016", vitessResultSet.getString("col17"));
        Assert.assertEquals("1234.56789", vitessResultSet.getString("col18"));
        Assert.assertEquals("HELLO TDS TEAM", vitessResultSet.getString("col19"));
        Assert.assertEquals("HELLO TDS TEAM", vitessResultSet.getString("col20"));
        Assert.assertEquals("HELLO TDS TEAM", vitessResultSet.getString("col21"));
        Assert.assertEquals("HELLO TDS TEAM", vitessResultSet.getString("col22"));
        Assert.assertEquals("N", vitessResultSet.getString("col23"));
        Assert.assertEquals("HELLO TDS TEAM", vitessResultSet.getString("col24"));
        Assert.assertEquals("1", vitessResultSet.getString("col25"));
        Assert.assertEquals("val123", vitessResultSet.getString("col26"));
        Assert.assertEquals("val123", vitessResultSet.getString("col27"));
    }

    @Test public void testgetBooleanbyColumnLabel() throws SQLException {
        Cursor cursor = getCursorWithRows();
        VitessResultSet vitessResultSet = new VitessResultSet(cursor, getVitessStatement());
        vitessResultSet.next();
        Assert.assertEquals(true, vitessResultSet.getBoolean("col25"));
        Assert.assertEquals(false, vitessResultSet.getBoolean("col1"));
    }

    @Test public void testgetBytebyColumnLabel() throws SQLException {
        Cursor cursor = getCursorWithRows();
        VitessResultSet vitessResultSet = new VitessResultSet(cursor, getVitessStatement());
        vitessResultSet.next();
        Assert.assertEquals(-50, vitessResultSet.getByte("col1"));
        Assert.assertEquals(1, vitessResultSet.getByte("col25"));
    }

    @Test public void testgetShortbyColumnLabel() throws SQLException {
        Cursor cursor = getCursorWithRows();
        VitessResultSet vitessResultSet = new VitessResultSet(cursor, getVitessStatement());
        vitessResultSet.next();
        Assert.assertEquals(-23000, vitessResultSet.getShort("col3"));
    }

    @Test public void testgetIntbyColumnLabel() throws SQLException {
        Cursor cursor = getCursorWithRows();
        VitessResultSet vitessResultSet = new VitessResultSet(cursor, getVitessStatement());
        vitessResultSet.next();
        Assert.assertEquals(-100, vitessResultSet.getInt("col7"));
    }

    @Test public void testgetLongbyColumnLabel() throws SQLException {
        Cursor cursor = getCursorWithRows();
        VitessResultSet vitessResultSet = new VitessResultSet(cursor, getVitessStatement());
        vitessResultSet.next();
        Assert.assertEquals(-1000, vitessResultSet.getInt("col9"));
    }

    @Test public void testgetFloatbyColumnLabel() throws SQLException {
        Cursor cursor = getCursorWithRows();
        VitessResultSet vitessResultSet = new VitessResultSet(cursor, getVitessStatement());
        vitessResultSet.next();
        Assert.assertEquals(24.52f, vitessResultSet.getFloat("col11"), 0.001);
    }

    @Test public void testgetDoublebyColumnLabel() throws SQLException {
        Cursor cursor = getCursorWithRows();
        VitessResultSet vitessResultSet = new VitessResultSet(cursor, getVitessStatement());
        vitessResultSet.next();
        Assert.assertEquals(100.43, vitessResultSet.getFloat("col12"), 0.001);
    }

    @Test public void testBigDecimalbyColumnLabel() throws SQLException {
        Cursor cursor = getCursorWithRows();
        VitessResultSet vitessResultSet = new VitessResultSet(cursor, getVitessStatement());
        vitessResultSet.next();
        Assert.assertEquals(new BigDecimal(BigInteger.valueOf(123456789), 5),
            vitessResultSet.getBigDecimal("col18"));
    }

    @Test public void testgetBytesbyColumnLabel()
        throws SQLException, UnsupportedEncodingException {
        Cursor cursor = getCursorWithRows();
        VitessResultSet vitessResultSet = new VitessResultSet(cursor, getVitessStatement());
        vitessResultSet.next();
        Assert.assertArrayEquals("HELLO TDS TEAM".getBytes("UTF-8"),
            vitessResultSet.getBytes("col19"));
    }

    @Test public void testgetDatebyColumnLabel() throws SQLException, UnsupportedEncodingException {
        Cursor cursor = getCursorWithRows();
        VitessResultSet vitessResultSet = new VitessResultSet(cursor, getVitessStatement());
        vitessResultSet.next();
        Assert.assertEquals(new java.sql.Date(116, 1, 6), vitessResultSet.getDate("col14"));
    }

    @Test public void testgetTimebyColumnLabel() throws SQLException, UnsupportedEncodingException {
        Cursor cursor = getCursorWithRows();
        VitessResultSet vitessResultSet = new VitessResultSet(cursor, getVitessStatement());
        vitessResultSet.next();
        Assert.assertEquals(new Time(12, 34, 56), vitessResultSet.getTime("col15"));
    }

    @Test public void testgetTimestampbyColumnLabel()
        throws SQLException, UnsupportedEncodingException {
        Cursor cursor = getCursorWithRows();
        VitessResultSet vitessResultSet = new VitessResultSet(cursor, getVitessStatement());
        vitessResultSet.next();
        Assert.assertEquals(new Timestamp(116, 1, 6, 14, 15, 16, 0),
            vitessResultSet.getTimestamp("col13"));
    }

    @Test public void testgetAsciiStream() throws SQLException, UnsupportedEncodingException {
        Cursor cursor = getCursorWithRows();
        VitessResultSet vitessResultSet = new VitessResultSet(cursor);
        vitessResultSet.next();
        // Need to implement AssertEquivalant
        //Assert.assertEquals((InputStream)(new ByteArrayInputStream("HELLO TDS TEAM".getBytes())), vitessResultSet
        // .getAsciiStream(19));
    }

    @Test public void testEnhancedFieldsFromCursor() throws Exception {
        Cursor cursor = getCursorWithRows();
        VitessResultSet vitessResultSet = new VitessResultSet(cursor, getVitessStatement());
        Assert.assertEquals(cursor.getFields().size(), vitessResultSet.getFields().size());
    }

    @Test public void testGetStringUsesEncoding() throws Exception {
        VitessConnection conn = getVitessConnection();
        VitessResultSet resultOne = PowerMockito.spy(new VitessResultSet(getCursorWithRows(), new VitessStatement(conn)));
        resultOne.next();
        // test all ways to get to convertBytesToString

        // Verify that we're going through convertBytesToString for column types that return bytes (string-like),
        // but not for those that return a real object
        resultOne.getString("col21"); // is a string, should go through convert bytes
        resultOne.getString("col13"); // is a datetime, should not
        PowerMockito.verifyPrivate(resultOne, VerificationModeFactory.times(1)).invoke("convertBytesToString", Matchers.any(byte[].class), Matchers.anyString());

        conn.setIncludedFields(Query.ExecuteOptions.IncludedFields.TYPE_AND_NAME);
        VitessResultSet resultTwo = PowerMockito.spy(new VitessResultSet(getCursorWithRows(), new VitessStatement(conn)));
        resultTwo.next();

        // neither of these should go through convertBytesToString, because we didn't include all fields
        resultTwo.getString("col21");
        resultTwo.getString("col13");
        PowerMockito.verifyPrivate(resultTwo, VerificationModeFactory.times(0)).invoke("convertBytesToString", Matchers.any(byte[].class), Matchers.anyString());
    }

    @Test public void testGetObjectForBitValues() throws Exception {
        VitessConnection conn = getVitessConnection();

        ByteString.Output value = ByteString.newOutput();
        value.write(new byte[] {1});
        value.write(new byte[] {0});
        value.write(new byte[] {1,2,3,4});

        Query.QueryResult result = Query.QueryResult.newBuilder()
            .addFields(Query.Field.newBuilder().setName("col1").setColumnLength(1).setType(Query.Type.BIT))
            .addFields(Query.Field.newBuilder().setName("col2").setColumnLength(1).setType(Query.Type.BIT))
            .addFields(Query.Field.newBuilder().setName("col3").setColumnLength(4).setType(Query.Type.BIT))
            .addRows(Query.Row.newBuilder()
                .addLengths(1)
                .addLengths(1)
                .addLengths(4)
                .setValues(value.toByteString()))
            .build();

        VitessResultSet vitessResultSet = PowerMockito.spy(new VitessResultSet(new SimpleCursor(result), new VitessStatement(conn)));
        vitessResultSet.next();

        Assert.assertEquals(true, vitessResultSet.getObject(1));
        Assert.assertEquals(false, vitessResultSet.getObject(2));
        Assert.assertArrayEquals(new byte[] {1,2,3,4}, (byte[]) vitessResultSet.getObject(3));

        PowerMockito.verifyPrivate(vitessResultSet, VerificationModeFactory.times(3)).invoke("convertBytesIfPossible", Matchers.any(byte[].class), Matchers.any(FieldWithMetadata.class));

        conn.setIncludedFields(Query.ExecuteOptions.IncludedFields.TYPE_AND_NAME);
        vitessResultSet = PowerMockito.spy(new VitessResultSet(new SimpleCursor(result), new VitessStatement(conn)));
        vitessResultSet.next();

        Assert.assertArrayEquals(new byte[] { 1 }, (byte[]) vitessResultSet.getObject(1));
        Assert.assertArrayEquals(new byte[] { 0 }, (byte[]) vitessResultSet.getObject(2));
        Assert.assertArrayEquals(new byte[] {1,2,3,4}, (byte[]) vitessResultSet.getObject(3));

        PowerMockito.verifyPrivate(vitessResultSet, VerificationModeFactory.times(0)).invoke("convertBytesIfPossible", Matchers.any(byte[].class), Matchers.any(FieldWithMetadata.class));
    }

    @Test public void testGetObjectForVarBinLikeValues() throws Exception {
        VitessConnection conn = getVitessConnection();

        ByteString.Output value = ByteString.newOutput();

        byte[] binary = new byte[] {1,2,3,4};
        byte[] varbinary = new byte[] {1,2,3,4,5,6,7,8,9,10,11,12,13};
        byte[] blob = new byte[MysqlDefs.LENGTH_BLOB];
        for (int i = 0; i < blob.length; i++) {
            blob[i] = 1;
        }
        byte[] fakeGeometry = new byte[] {2,3,4};

        value.write(binary);
        value.write(varbinary);
        value.write(blob);
        value.write(fakeGeometry);

        Query.QueryResult result = Query.QueryResult.newBuilder()
            .addFields(Query.Field.newBuilder().setName("col1")
                .setColumnLength(4)
                .setCharset(CharsetMapping.MYSQL_COLLATION_INDEX_binary)
                .setType(Query.Type.BINARY)
                .setFlags(Query.MySqlFlag.BINARY_FLAG_VALUE))
            .addFields(Query.Field.newBuilder().setName("col2")
                .setColumnLength(13)
                .setCharset(CharsetMapping.MYSQL_COLLATION_INDEX_binary)
                .setType(Query.Type.VARBINARY)
                .setFlags(Query.MySqlFlag.BINARY_FLAG_VALUE))
            .addFields(Query.Field.newBuilder().setName("col3") // should go to LONGVARBINARY due to below settings
                .setColumnLength(MysqlDefs.LENGTH_BLOB)
                .setCharset(CharsetMapping.MYSQL_COLLATION_INDEX_binary)
                .setFlags(Query.MySqlFlag.BINARY_FLAG_VALUE)
                .setType(Query.Type.BLOB))
            .addFields(Query.Field.newBuilder().setName("col4")
                .setType(Query.Type.GEOMETRY)
                .setCharset(CharsetMapping.MYSQL_COLLATION_INDEX_binary)
                .setType(Query.Type.BINARY)
                .setFlags(Query.MySqlFlag.BINARY_FLAG_VALUE))
            .addRows(Query.Row.newBuilder()
                .addLengths(4)
                .addLengths(13)
                .addLengths(MysqlDefs.LENGTH_BLOB)
                .addLengths(3)
                .setValues(value.toByteString()))
            .build();

        VitessResultSet vitessResultSet = PowerMockito.spy(new VitessResultSet(new SimpleCursor(result), new VitessStatement(conn)));
        vitessResultSet.next();

        // All of these types should pass straight through, returning the direct bytes
        Assert.assertArrayEquals(binary, (byte[]) vitessResultSet.getObject(1));
        Assert.assertArrayEquals(varbinary, (byte[]) vitessResultSet.getObject(2));
        Assert.assertArrayEquals(blob, (byte[]) vitessResultSet.getObject(3));
        Assert.assertArrayEquals(fakeGeometry, (byte[]) vitessResultSet.getObject(4));

        // We should still call the function 4 times
        PowerMockito.verifyPrivate(vitessResultSet, VerificationModeFactory.times(4)).invoke("convertBytesIfPossible", Matchers.any(byte[].class), Matchers.any(FieldWithMetadata.class));

        conn.setIncludedFields(Query.ExecuteOptions.IncludedFields.TYPE_AND_NAME);
        vitessResultSet = PowerMockito.spy(new VitessResultSet(new SimpleCursor(result), new VitessStatement(conn)));
        vitessResultSet.next();

        // Same as above since this doesn't really do much but pass right through for the varbinary type
        Assert.assertArrayEquals(binary, (byte[]) vitessResultSet.getObject(1));
        Assert.assertArrayEquals(varbinary, (byte[]) vitessResultSet.getObject(2));
        Assert.assertArrayEquals(blob, (byte[]) vitessResultSet.getObject(3));
        Assert.assertArrayEquals(fakeGeometry, (byte[]) vitessResultSet.getObject(4));

        // Never call because not including all
        PowerMockito.verifyPrivate(vitessResultSet, VerificationModeFactory.times(0)).invoke("convertBytesIfPossible", Matchers.any(byte[].class), Matchers.any(FieldWithMetadata.class));
    }

    @Test public void testGetObjectForStringLikeValues() throws Exception {
        ByteString.Output value = ByteString.newOutput();

        String trimmedCharStr = "wasting space";
        String varcharStr = "i have a variable length!";
        String masqueradingBlobStr = "look at me, im a blob";
        String textStr = "an enthralling string of TEXT in some foreign language";

        int paddedCharColLength = 20;
        byte[] trimmedChar = StringUtils.getBytes(trimmedCharStr, "UTF-16");
        byte[] varchar = StringUtils.getBytes(varcharStr, "UTF-8");
        byte[] masqueradingBlob = StringUtils.getBytes(masqueradingBlobStr, "US-ASCII");
        byte[] text = StringUtils.getBytes(textStr, "ISO8859_8");
        byte[] opaqueBinary = new byte[] { 1,2,3,4,5,6,7,8,9};

        value.write(trimmedChar);
        value.write(varchar);
        value.write(opaqueBinary);
        value.write(masqueradingBlob);
        value.write(text);

        Query.QueryResult result = Query.QueryResult.newBuilder()
            // This tests CHAR
            .addFields(Query.Field.newBuilder().setName("col1")
                .setColumnLength(paddedCharColLength)
                .setCharset(/* utf-16 collation index from CharsetMapping */ 54)
                .setType(Query.Type.CHAR))
            // This tests VARCHAR
            .addFields(Query.Field.newBuilder().setName("col2")
                .setColumnLength(varchar.length)
                .setCharset(CharsetMapping.MYSQL_COLLATION_INDEX_utf8)
                .setType(Query.Type.VARCHAR))
            // This tests VARCHAR that is an opaque binary
            .addFields(Query.Field.newBuilder().setName("col2")
                .setColumnLength(opaqueBinary.length)
                .setCharset(CharsetMapping.MYSQL_COLLATION_INDEX_binary)
                .setFlags(Query.MySqlFlag.BINARY_FLAG_VALUE)
                .setType(Query.Type.VARCHAR))
            // This tests LONGVARCHAR
            .addFields(Query.Field.newBuilder().setName("col3")
                .setColumnLength(masqueradingBlob.length)
                .setCharset(/* us-ascii collation index from CharsetMapping */11)
                .setType(Query.Type.BLOB))
            // This tests TEXT, which falls through the default case of the switch
            .addFields(Query.Field.newBuilder().setName("col4")
                .setColumnLength(text.length)
                .setCharset(/* corresponds to greek, from CharsetMapping */25)
                .setType(Query.Type.TEXT))
            .addRows(Query.Row.newBuilder()
                .addLengths(trimmedChar.length)
                .addLengths(varchar.length)
                .addLengths(opaqueBinary.length)
                .addLengths(masqueradingBlob.length)
                .addLengths(text.length)
                .setValues(value.toByteString()))
            .build();

        VitessConnection conn = getVitessConnection();
        VitessResultSet vitessResultSet = PowerMockito.spy(new VitessResultSet(new SimpleCursor(result), new VitessStatement(conn)));
        vitessResultSet.next();

        Assert.assertEquals(trimmedCharStr, vitessResultSet.getObject(1));
        Assert.assertEquals(varcharStr, vitessResultSet.getObject(2));
        Assert.assertArrayEquals(opaqueBinary, (byte[]) vitessResultSet.getObject(3));
        Assert.assertEquals(masqueradingBlobStr, vitessResultSet.getObject(4));
        Assert.assertEquals(textStr, vitessResultSet.getObject(5));

        PowerMockito.verifyPrivate(vitessResultSet, VerificationModeFactory.times(5)).invoke("convertBytesIfPossible", Matchers.any(byte[].class), Matchers.any(FieldWithMetadata.class));

        conn.setIncludedFields(Query.ExecuteOptions.IncludedFields.TYPE_AND_NAME);
        vitessResultSet = PowerMockito.spy(new VitessResultSet(new SimpleCursor(result), new VitessStatement(conn)));
        vitessResultSet.next();

        Assert.assertArrayEquals(trimmedChar, (byte[]) vitessResultSet.getObject(1));
        Assert.assertArrayEquals(varchar, (byte[]) vitessResultSet.getObject(2));
        Assert.assertArrayEquals(opaqueBinary, (byte[]) vitessResultSet.getObject(3));
        Assert.assertArrayEquals(masqueradingBlob, (byte[]) vitessResultSet.getObject(4));
        Assert.assertArrayEquals(text, (byte[]) vitessResultSet.getObject(5));

        PowerMockito.verifyPrivate(vitessResultSet, VerificationModeFactory.times(0)).invoke("convertBytesIfPossible", Matchers.any(byte[].class), Matchers.any(FieldWithMetadata.class));
    }
}
