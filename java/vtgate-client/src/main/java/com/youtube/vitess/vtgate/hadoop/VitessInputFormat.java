package com.youtube.vitess.vtgate.hadoop;

import com.youtube.vitess.vtgate.Exceptions.ConnectionException;
import com.youtube.vitess.vtgate.Exceptions.DatabaseException;
import com.youtube.vitess.vtgate.KeyspaceId;
import com.youtube.vitess.vtgate.Query;
import com.youtube.vitess.vtgate.VtGate;
import com.youtube.vitess.vtgate.hadoop.writables.KeyspaceIdWritable;
import com.youtube.vitess.vtgate.hadoop.writables.RowWritable;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * {@link VitessInputFormat} is the {@link InputFormat} for tables in Vitess. Input splits (
 * {@link VitessInputSplit}) are fetched from VtGate via an RPC. map() calls are supplied with a
 * {@link KeyspaceIdWritable}, {@link RowWritable} pair.
 */
public class VitessInputFormat extends InputFormat<NullWritable, RowWritable> {

  @Override
  public List<InputSplit> getSplits(JobContext context) {
    try {
      VitessConf conf = new VitessConf(context.getConfiguration());
      VtGate vtgate = VtGate.connect(conf.getHosts(), conf.getTimeoutMs());
      List<String> columns = conf.getInputColumns();
      if (!columns.contains(KeyspaceId.COL_NAME)) {
        columns.add(KeyspaceId.COL_NAME);
      }
      String sql = "select " + StringUtils.join(columns, ',') + " from " + conf.getInputTable();
      Map<Query, Long> queries =
          vtgate.splitQuery(conf.getKeyspace(), sql, conf.getSplitsPerShard());
      List<InputSplit> splits = new LinkedList<>();
      for (Query query : queries.keySet()) {
        Long size = queries.get(query);
        InputSplit split = new VitessInputSplit(query, size);
        splits.add(split);
      }

      for (InputSplit split : splits) {
        ((VitessInputSplit) split).setLocations(conf.getHosts().split(VitessConf.HOSTS_DELIM));
      }
      return splits;
    } catch (ConnectionException | DatabaseException e) {
      throw new RuntimeException(e);
    }
  }

  public RecordReader<NullWritable, RowWritable> createRecordReader(InputSplit split,
      TaskAttemptContext context) throws IOException, InterruptedException {
    return new VitessRecordReader();
  }

  /**
   * Sets the necessary configurations for Vitess table input source
   */
  public static void setInput(Job job,
      String hosts,
      String keyspace,
      String table,
      List<String> columns,
      int splitsPerShard) {
    job.setInputFormatClass(VitessInputFormat.class);
    VitessConf vtConf = new VitessConf(job.getConfiguration());
    vtConf.setHosts(hosts);
    vtConf.setKeyspace(keyspace);
    vtConf.setInputTable(table);
    vtConf.setInputColumns(columns);
    vtConf.setSplitsPerShard(splitsPerShard);
  }
}
