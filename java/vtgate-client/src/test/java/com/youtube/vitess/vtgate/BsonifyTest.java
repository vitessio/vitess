package com.youtube.vitess.vtgate;

import com.google.common.primitives.UnsignedLong;

import com.youtube.vitess.vtgate.Exceptions.InvalidFieldException;
import com.youtube.vitess.vtgate.Row.Cell;
import com.youtube.vitess.vtgate.cursor.Cursor;
import com.youtube.vitess.vtgate.cursor.CursorImpl;
import com.youtube.vitess.vtgate.rpcclient.gorpc.Bsonify;

import org.bson.BSONObject;
import org.bson.BasicBSONObject;
import org.bson.types.BasicBSONList;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.math.BigDecimal;

@RunWith(JUnit4.class)
public class BsonifyTest {

  @Test
  public void testResultParse() throws InvalidFieldException {
    BSONObject result = new BasicBSONObject();
    result.put("RowsAffected", UnsignedLong.valueOf("12"));
    result.put("InsertId", UnsignedLong.valueOf("12345"));
    BasicBSONList fields = new BasicBSONList();
    for (long l = 0; l < 4; l++) {
      BSONObject field = new BasicBSONObject();
      field.put("Name", ("col_" + l).getBytes());
      field.put("Type", l);
      fields.add(field);
    }
    result.put("Fields", fields);
    BasicBSONList rows = new BasicBSONList();
    for (int i = 0; i < 3; i++) {
      BasicBSONList row = new BasicBSONList();
      row.add(new Double(i).toString().getBytes());
      row.add(String.valueOf(i).getBytes());
      row.add(String.valueOf(i).getBytes());
      row.add(new Long(i).toString().getBytes());
      rows.add(row);
    }
    result.put("Rows", rows);

    QueryResult qr = Bsonify.bsonToQueryResult(result, null);
    Cursor cursor = new CursorImpl(qr);
    Assert.assertEquals(12L, cursor.getRowsAffected());
    Assert.assertEquals(12345L, cursor.getLastRowId());

    Row firstRow = cursor.next();
    Cell cell0 = firstRow.next();
    Assert.assertEquals("col_0", cell0.getName());
    Assert.assertEquals(BigDecimal.class, cell0.getType());
    Assert.assertEquals(new BigDecimal("0.0"), firstRow.getBigDecimal(cell0.getName()));

    Cell cell1 = firstRow.next();
    Assert.assertEquals("col_1", cell1.getName());
    Assert.assertEquals(Integer.class, cell1.getType());
    Assert.assertEquals(new Integer(0), firstRow.getInt(cell1.getName()));

    Cell cell2 = firstRow.next();
    Assert.assertEquals("col_2", cell2.getName());
    Assert.assertEquals(Integer.class, cell2.getType());
    Assert.assertEquals(new Integer(0), firstRow.getInt(cell2.getName()));

    Cell cell3 = firstRow.next();
    Assert.assertEquals("col_3", cell3.getName());
    Assert.assertEquals(Long.class, cell3.getType());
    Assert.assertEquals(new Long(0), firstRow.getLong(cell3.getName()));
  }
}
