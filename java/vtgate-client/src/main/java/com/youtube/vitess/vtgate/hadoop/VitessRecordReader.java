package com.youtube.vitess.vtgate.hadoop;

import com.youtube.vitess.vtgate.Exceptions.ConnectionException;
import com.youtube.vitess.vtgate.Exceptions.DatabaseException;
import com.youtube.vitess.vtgate.Row;
import com.youtube.vitess.vtgate.VtGate;
import com.youtube.vitess.vtgate.cursor.Cursor;
import com.youtube.vitess.vtgate.hadoop.writables.RowWritable;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

public class VitessRecordReader extends RecordReader<NullWritable, RowWritable> {
  private VitessInputSplit split;
  private VtGate vtgate;
  private VitessConf vtConf;
  private RowWritable rowWritable;
  private long rowsProcessed = 0;
  private Cursor cursor;

  /**
   * Fetch connection parameters from Configuraiton and open VtGate connection.
   */
  @Override
  public void initialize(InputSplit split, TaskAttemptContext context) throws IOException,
      InterruptedException {
    this.split = (VitessInputSplit) split;
    vtConf = new VitessConf(context.getConfiguration());
    try {
      vtgate = VtGate.connect(vtConf.getHosts(), vtConf.getTimeoutMs());
    } catch (ConnectionException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void close() throws IOException {
    if (vtgate != null) {
      try {
        vtgate.close();
        vtgate = null;
      } catch (ConnectionException e) {
        throw new RuntimeException(e);
      }
    }
  }

  @Override
  public NullWritable getCurrentKey() throws IOException, InterruptedException {
    return NullWritable.get();
  }

  @Override
  public RowWritable getCurrentValue() throws IOException, InterruptedException {
    return rowWritable;
  }

  @Override
  public float getProgress() throws IOException, InterruptedException {
    if (rowsProcessed > split.getLength()) {
      return 0.9f;
    }
    return rowsProcessed / split.getLength();
  }

  /**
   * Fetches the next row. If this is the first invocation for the split, execute the streaming
   * query. Subsequent calls just advance the iterator.
   */
  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException {
    if (cursor == null) {
      try {
        cursor = vtgate.execute(split.getQuery());
      } catch (DatabaseException | ConnectionException e) {
        throw new RuntimeException(e);
      }
    }
    if (!cursor.hasNext()) {
      return false;
    }
    Row row = cursor.next();
    rowWritable = new RowWritable(row);
    rowsProcessed++;
    return true;
  }
}
