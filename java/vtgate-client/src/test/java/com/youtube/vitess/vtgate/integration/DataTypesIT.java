package com.youtube.vitess.vtgate.integration;

import com.google.common.collect.Lists;
import com.google.common.primitives.UnsignedLong;

import com.youtube.vitess.vtgate.BindVariable;
import com.youtube.vitess.vtgate.KeyRange;
import com.youtube.vitess.vtgate.Query;
import com.youtube.vitess.vtgate.Query.QueryBuilder;
import com.youtube.vitess.vtgate.Row;
import com.youtube.vitess.vtgate.VtGate;
import com.youtube.vitess.vtgate.cursor.Cursor;
import com.youtube.vitess.vtgate.integration.util.TestEnv;
import com.youtube.vitess.vtgate.integration.util.Util;

import org.joda.time.DateTime;
import org.joda.time.format.ISODateTimeFormat;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RunWith(JUnit4.class)
public class DataTypesIT {

  public static TestEnv testEnv = getTestEnv();

  @BeforeClass
  public static void setUpVtGate() throws Exception {
    Util.setupTestEnv(testEnv, true);
  }

  @AfterClass
  public static void tearDownVtGate() throws Exception {
    Util.setupTestEnv(testEnv, false);
  }

  @Test
  public void testInts() throws Exception {
    String createTable =
        "create table vtocc_ints(tiny tinyint, tinyu tinyint unsigned, small smallint, smallu smallint unsigned, medium mediumint, mediumu mediumint unsigned, normal int, normalu int unsigned, big bigint, bigu bigint unsigned, year year, primary key(tiny)) comment 'vtocc_nocache'\n" + "";
    VtGate vtgate = VtGate.connect("localhost:" + testEnv.port, 0);
    vtgate.begin();
    vtgate.execute(getQuery(createTable));
    vtgate.commit();

    String insertSql =
        "insert into vtocc_ints values(:tiny, :tinyu, :small, :smallu, :medium, :mediumu, :normal, :normalu, :big, :bigu, :year)";
    Query insertQuery = new QueryBuilder(insertSql, testEnv.keyspace, "master")
        .addBindVar(BindVariable.forInt("tiny", -128))
        .addBindVar(BindVariable.forInt("tinyu", 255))
        .addBindVar(BindVariable.forInt("small", -32768))
        .addBindVar(BindVariable.forInt("smallu", 65535))
        .addBindVar(BindVariable.forInt("medium", -8388608))
        .addBindVar(BindVariable.forInt("mediumu", 16777215))
        .addBindVar(BindVariable.forLong("normal", -2147483648L))
        .addBindVar(BindVariable.forLong("normalu", 4294967295L))
        .addBindVar(BindVariable.forLong("big", -9223372036854775808L))
        .addBindVar(BindVariable.forULong("bigu", UnsignedLong.valueOf("18446744073709551615")))
        .addBindVar(BindVariable.forShort("year", (short) 2012))
        .addKeyRange(KeyRange.ALL)
        .build();
    vtgate.begin();
    vtgate.execute(insertQuery);
    vtgate.commit();

    String selectSql = "select * from vtocc_ints where tiny = -128";
    Query selectQuery =
        new QueryBuilder(selectSql, testEnv.keyspace, "master").addKeyRange(KeyRange.ALL).build();
    Cursor cursor = vtgate.execute(selectQuery);
    Assert.assertEquals(1, cursor.getRowsAffected());
    Row row = cursor.next();
    Assert.assertTrue(row.getInt("tiny").equals(-128));
    Assert.assertTrue(row.getInt("tinyu").equals(255));
    Assert.assertTrue(row.getInt("small").equals(-32768));
    Assert.assertTrue(row.getInt("smallu").equals(65535));
    Assert.assertTrue(row.getInt("medium").equals(-8388608));
    Assert.assertTrue(row.getInt("mediumu").equals(16777215));
    Assert.assertTrue(row.getLong("normal").equals(-2147483648L));
    Assert.assertTrue(row.getLong("normalu").equals(4294967295L));
    Assert.assertTrue(row.getLong("big").equals(-9223372036854775808L));
    Assert.assertTrue(row.getULong("bigu").equals(UnsignedLong.valueOf("18446744073709551615")));
    Assert.assertTrue(row.getShort("year").equals((short) 2012));
    vtgate.close();
  }

  @Test
  public void testFracts() throws Exception {
    String createTable =
        "create table vtocc_fracts(id int, deci decimal(5,2), num numeric(5,2), f float, d double, primary key(id)) comment 'vtocc_nocache'\n" + "";
    VtGate vtgate = VtGate.connect("localhost:" + testEnv.port, 0);
    vtgate.begin();
    vtgate.execute(getQuery(createTable));
    vtgate.commit();

    String insertSql = "insert into vtocc_fracts values(:id, :deci, :num, :f, :d)";
    Query insertQuery = new QueryBuilder(insertSql, testEnv.keyspace, "master")
        .addBindVar(BindVariable.forInt("id", 1))
        .addBindVar(BindVariable.forDouble("deci", 1.99))
        .addBindVar(BindVariable.forDouble("num", 2.99))
        .addBindVar(BindVariable.forFloat("f", 3.99F))
        .addBindVar(BindVariable.forDouble("d", 4.99))
        .addKeyRange(KeyRange.ALL)
        .build();
    vtgate.begin();
    vtgate.execute(insertQuery);
    vtgate.commit();

    String selectSql = "select * from vtocc_fracts where id = 1";
    Query selectQuery =
        new QueryBuilder(selectSql, testEnv.keyspace, "master").addKeyRange(KeyRange.ALL).build();
    Cursor cursor = vtgate.execute(selectQuery);
    Assert.assertEquals(1, cursor.getRowsAffected());
    Row row = cursor.next();
    Assert.assertTrue(row.getLong("id").equals(1L));
    Assert.assertTrue(row.getBigDecimal("deci").equals(new BigDecimal("1.99")));
    Assert.assertTrue(row.getBigDecimal("num").equals(new BigDecimal("2.99")));
    Assert.assertTrue(row.getFloat("f").equals(3.99F));
    Assert.assertTrue(row.getDouble("d").equals(4.99));
    vtgate.close();
  }

  @Test
  public void testStrings() throws Exception {
    String createTable =
        "create table vtocc_strings(vb varbinary(16), c char(16), vc varchar(16), b binary(4), tb tinyblob, bl blob, ttx tinytext, tx text, en enum('a','b'), s set('a','b'), primary key(vb)) comment 'vtocc_nocache'\n" + "";
    VtGate vtgate = VtGate.connect("localhost:" + testEnv.port, 0);
    vtgate.begin();
    vtgate.execute(getQuery(createTable));
    vtgate.commit();

    String insertSql =
        "insert into vtocc_strings values(:vb, :c, :vc, :b, :tb, :bl, :ttx, :tx, :en, :s)";
    Query insertQuery = new QueryBuilder(insertSql, testEnv.keyspace, "master")
        .addBindVar(BindVariable.forBytes("vb", "a".getBytes()))
        .addBindVar(BindVariable.forBytes("c", "b".getBytes()))
        .addBindVar(BindVariable.forBytes("vc", "c".getBytes()))
        .addBindVar(BindVariable.forBytes("b", "d".getBytes()))
        .addBindVar(BindVariable.forBytes("tb", "e".getBytes()))
        .addBindVar(BindVariable.forBytes("bl", "f".getBytes()))
        .addBindVar(BindVariable.forBytes("ttx", "g".getBytes()))
        .addBindVar(BindVariable.forBytes("tx", "h".getBytes()))
        .addBindVar(BindVariable.forString("en", "a"))
        .addBindVar(BindVariable.forString("s", "a,b"))
        .addKeyRange(KeyRange.ALL)
        .build();
    vtgate.begin();
    vtgate.execute(insertQuery);
    vtgate.commit();

    String selectSql = "select * from vtocc_strings where vb = 'a'";
    Query selectQuery =
        new QueryBuilder(selectSql, testEnv.keyspace, "master").addKeyRange(KeyRange.ALL).build();
    Cursor cursor = vtgate.execute(selectQuery);
    Assert.assertEquals(1, cursor.getRowsAffected());
    Row row = cursor.next();
    Assert.assertTrue(Arrays.equals(row.getBytes("vb"), "a".getBytes()));
    Assert.assertTrue(Arrays.equals(row.getBytes("c"), "b".getBytes()));
    Assert.assertTrue(Arrays.equals(row.getBytes("vc"), "c".getBytes()));
    // binary(4) column will be suffixed with three 0 bytes
    Assert.assertTrue(
        Arrays.equals(row.getBytes("b"), ByteBuffer.allocate(4).put((byte) 'd').array()));
    Assert.assertTrue(Arrays.equals(row.getBytes("tb"), "e".getBytes()));
    Assert.assertTrue(Arrays.equals(row.getBytes("bl"), "f".getBytes()));
    Assert.assertTrue(Arrays.equals(row.getBytes("ttx"), "g".getBytes()));
    Assert.assertTrue(Arrays.equals(row.getBytes("tx"), "h".getBytes()));
    // Assert.assertTrue(row.getString("en").equals("a"));
    // Assert.assertTrue(row.getString("s").equals("a,b"));
    vtgate.close();
  }

  @Test
  public void testMisc() throws Exception {
    String createTable =
        "create table vtocc_misc(id int, b bit(8), d date, dt datetime, t time, primary key(id)) comment 'vtocc_nocache'\n" + "";
    VtGate vtgate = VtGate.connect("localhost:" + testEnv.port, 0);
    vtgate.begin();
    vtgate.execute(getQuery(createTable));
    vtgate.commit();

    String insertSql = "insert into vtocc_misc values(:id, :b, :d, :dt, :t)";
    Query insertQuery = new QueryBuilder(insertSql, testEnv.keyspace, "master")
        .addBindVar(BindVariable.forInt("id", 1))
        .addBindVar(BindVariable.forBytes("b", ByteBuffer.allocate(1).put((byte) 1).array()))
        .addBindVar(BindVariable.forDate("d", DateTime.parse("2012-01-01")))
        .addBindVar(BindVariable.forDateTime("dt", DateTime.parse("2012-01-01T15:45:45")))
        .addBindVar(BindVariable.forTime("t",
            DateTime.parse("15:45:45", ISODateTimeFormat.timeElementParser())))
        .addKeyRange(KeyRange.ALL)
        .build();
    vtgate.begin();
    vtgate.execute(insertQuery);
    vtgate.commit();

    String selectSql = "select * from vtocc_misc where id = 1";
    Query selectQuery =
        new QueryBuilder(selectSql, testEnv.keyspace, "master").addKeyRange(KeyRange.ALL).build();
    Cursor cursor = vtgate.execute(selectQuery);
    Assert.assertEquals(1, cursor.getRowsAffected());
    Row row = cursor.next();
    Assert.assertTrue(row.getLong("id").equals(1L));
    Assert.assertTrue(
        Arrays.equals(row.getBytes("b"), ByteBuffer.allocate(1).put((byte) 1).array()));
    Assert.assertTrue(row.getDateTime("d").equals(DateTime.parse("2012-01-01")));
    Assert.assertTrue(row.getDateTime("dt").equals(DateTime.parse("2012-01-01T15:45:45")));
    Assert.assertTrue(row.getDateTime("t").equals(
        DateTime.parse("15:45:45", ISODateTimeFormat.timeElementParser())));
    vtgate.close();
  }

  private Query getQuery(String sql) {
    return new QueryBuilder(sql, testEnv.keyspace, "master").addKeyRange(KeyRange.ALL).build();
  }

  /**
   * Create env with two shards each having a master and replica
   */
  static TestEnv getTestEnv() {
    Map<String, List<String>> shardKidMap = new HashMap<>();
    shardKidMap.put("-", Lists.newArrayList("527875958493693904"));
    TestEnv env = new TestEnv(shardKidMap, "test_keyspace");
    env.addTablet("replica", 1);
    return env;
  }
}
