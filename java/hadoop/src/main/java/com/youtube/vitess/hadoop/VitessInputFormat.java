package com.youtube.vitess.hadoop;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.api.client.util.Lists;
import com.google.common.net.HostAndPort;

import com.youtube.vitess.client.Context;
import com.youtube.vitess.client.RpcClient;
import com.youtube.vitess.client.RpcClientFactory;
import com.youtube.vitess.client.VTGateBlockingConn;
import com.youtube.vitess.proto.Vtgate.SplitQueryResponse;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.joda.time.Duration;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

/**
 * {@link VitessInputFormat} is the {@link org.apache.hadoop.mapreduce.InputFormat} for tables in
 * Vitess. Input splits ({@link VitessInputSplit}) are fetched from VtGate via an RPC. map() calls
 * are supplied with a {@link RowWritable}.
 */
public class VitessInputFormat extends InputFormat<NullWritable, RowWritable> {

  @Override
  public List<InputSplit> getSplits(JobContext context) {
    VitessConf conf = new VitessConf(context.getConfiguration());
    List<SplitQueryResponse.Part> splitResult;
    try {
      @SuppressWarnings("unchecked")
      Class<? extends RpcClientFactory> rpcFactoryClass =
          (Class<? extends RpcClientFactory>) Class.forName(conf.getRpcFactoryClass());
      List<String> addressList = Arrays.asList(conf.getHosts().split(","));
      int index = new Random().nextInt(addressList.size());
      HostAndPort hostAndPort = HostAndPort.fromString(addressList.get(index));

      RpcClient rpcClient = rpcFactoryClass.newInstance().create(
          Context.getDefault().withDeadlineAfter(Duration.millis(conf.getTimeoutMs())),
          new InetSocketAddress(hostAndPort.getHostText(), hostAndPort.getPort()));

      try (VTGateBlockingConn vtgate = new VTGateBlockingConn(rpcClient)) {
        splitResult = vtgate.splitQuery(Context.getDefault(), conf.getKeyspace(),
            conf.getInputQuery(), null, conf.getSplitColumn(), conf.getSplits());
      }
    } catch (ClassNotFoundException | InstantiationException | IllegalAccessException | SQLException
        | IOException e) {
      throw new RuntimeException(e);
    }

    List<InputSplit> splits = Lists.newArrayList();
    for (SplitQueryResponse.Part part : splitResult) {
      splits.add(new VitessInputSplit(part));
    }

    for (InputSplit split : splits) {
      ((VitessInputSplit) split).setLocations(conf.getHosts().split(VitessConf.HOSTS_DELIM));
    }
    return splits;
  }

  @Override
  public RecordReader<NullWritable, RowWritable> createRecordReader(InputSplit split,
      TaskAttemptContext context) throws IOException, InterruptedException {
    return new VitessRecordReader();
  }

  /**
   * Sets the necessary configurations for Vitess table input source
   */
  public static void setInput(Job job, String hosts, String keyspace, String query,
      String splitColumn, int splits, Class<? extends RpcClientFactory> rpcFactoryClass) {
    job.setInputFormatClass(VitessInputFormat.class);
    VitessConf vtConf = new VitessConf(job.getConfiguration());
    vtConf.setHosts(checkNotNull(hosts));
    vtConf.setKeyspace(checkNotNull(keyspace));
    vtConf.setInputQuery(checkNotNull(query));
    vtConf.setSplitColumn(checkNotNull(splitColumn));
    vtConf.setSplits(splits);
    vtConf.setRpcFactoryClass(checkNotNull(rpcFactoryClass));
  }
}
