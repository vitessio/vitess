package com.youtube.vitess.vtgate;

import com.google.common.primitives.UnsignedLong;

import com.youtube.vitess.vtgate.Exceptions.InvalidFieldException;
import com.youtube.vitess.vtgate.Field.FieldType;
import com.youtube.vitess.vtgate.Field.Flag;
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
    fields.add(newField("col_0", FieldType.VT_DECIMAL, Flag.VT_ZEROVALUE_FLAG));
    fields.add(newField("col_1", FieldType.VT_TINY, Flag.VT_ZEROVALUE_FLAG));
    fields.add(newField("col_2", FieldType.VT_SHORT, Flag.VT_ZEROVALUE_FLAG));
    fields.add(newField("col_3", FieldType.VT_LONG, Flag.VT_ZEROVALUE_FLAG));
    fields.add(newField("col_4", FieldType.VT_LONGLONG, Flag.VT_ZEROVALUE_FLAG));
    fields.add(newField("col_5", FieldType.VT_LONGLONG, Flag.VT_UNSIGNED_FLAG));
    result.put("Fields", fields);
    
    // Fill each column with the following different values: 0, 1, 2
    BasicBSONList rows = new BasicBSONList();
    int rowCount = 2;
    for (int i = 0; i <= rowCount; i++) {
      BasicBSONList row = new BasicBSONList();
      row.add(new Double(i).toString().getBytes());
      row.add(String.valueOf(i).getBytes());
      row.add(String.valueOf(i).getBytes());
      row.add(new Long(i).toString().getBytes());
      row.add(new Long(i).toString().getBytes());
      row.add(new Long(i).toString().getBytes());
      Assert.assertEquals(fields.size(), row.size());
      rows.add(row);
    }
    result.put("Rows", rows);

    QueryResult qr = Bsonify.bsonToQueryResult(result, null);
    Cursor cursor = new CursorImpl(qr);
    Assert.assertEquals(12L, cursor.getRowsAffected());
    Assert.assertEquals(12345L, cursor.getLastRowId());

    for (int i = 0; i <= rowCount; i++) {
      Row row = cursor.next();
      Cell cell0 = row.next();
      Assert.assertEquals("col_0", cell0.getName());
      Assert.assertEquals(BigDecimal.class, cell0.getType());
      Assert.assertEquals(new BigDecimal(String.format("%d.0", i)), row.getBigDecimal(cell0.getName()));
  
      Cell cell1 = row.next();
      Assert.assertEquals("col_1", cell1.getName());
      Assert.assertEquals(Integer.class, cell1.getType());
      Assert.assertEquals(new Integer(i), row.getInt(cell1.getName()));
  
      Cell cell2 = row.next();
      Assert.assertEquals("col_2", cell2.getName());
      Assert.assertEquals(Integer.class, cell2.getType());
      Assert.assertEquals(new Integer(i), row.getInt(cell2.getName()));
  
      Cell cell3 = row.next();
      Assert.assertEquals("col_3", cell3.getName());
      Assert.assertEquals(Long.class, cell3.getType());
      Assert.assertEquals(new Long(i), row.getLong(cell3.getName()));
  
      Cell cell4 = row.next();
      Assert.assertEquals("col_4", cell4.getName());
      Assert.assertEquals(Long.class, cell4.getType());
      Assert.assertEquals(new Long(i), row.getLong(cell4.getName()));
  
      Cell cell5 = row.next();
      Assert.assertEquals("col_5", cell5.getName());
      Assert.assertEquals(UnsignedLong.class, cell5.getType());
      Assert.assertEquals(UnsignedLong.valueOf(String.format("%d", i)), row.getULong(cell5.getName()));
    }
    // No more rows left.
    Assert.assertFalse(cursor.hasNext());
}

  private BSONObject newField(String name, FieldType fieldType, Flag flag) {
    BSONObject field = new BasicBSONObject();
    field.put("Name", name.getBytes());
    field.put("Type", (long) fieldType.mysqlType);
    field.put("Flags", (long) flag.mysqlFlag);
    
    return field;
  }
}
