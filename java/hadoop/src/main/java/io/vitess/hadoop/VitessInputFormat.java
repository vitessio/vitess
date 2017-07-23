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

package io.vitess.hadoop;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.joda.time.Duration;

import com.google.common.collect.Lists;

import io.vitess.client.Context;
import io.vitess.client.RpcClient;
import io.vitess.client.RpcClientFactory;
import io.vitess.client.VTGateBlockingConn;
import io.vitess.proto.Query.SplitQueryRequest.Algorithm;
import io.vitess.proto.Vtgate.SplitQueryResponse;

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

      RpcClient rpcClient = rpcFactoryClass.newInstance().create(
          Context.getDefault().withDeadlineAfter(Duration.millis(conf.getTimeoutMs())),
          addressList.get(index));

      try (VTGateBlockingConn vtgate = new VTGateBlockingConn(rpcClient)) {
        splitResult = vtgate.splitQuery(
            Context.getDefault(),
            conf.getKeyspace(),
            conf.getInputQuery(),
            null /* bind vars */,
            conf.getSplitColumns(),
            conf.getSplitCount(),
            conf.getNumRowsPerQueryPart(),
            conf.getAlgorithm());
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
  public static void setInput(
      Job job,
      String hosts,
      String keyspace,
      String query,
      Collection<String> splitColumns,
      int splitCount,
      int numRowsPerQueryPart,
      Algorithm algorithm,
      Class<? extends RpcClientFactory> rpcFactoryClass) {
    job.setInputFormatClass(VitessInputFormat.class);
    VitessConf vtConf = new VitessConf(job.getConfiguration());
    vtConf.setHosts(checkNotNull(hosts));
    vtConf.setKeyspace(checkNotNull(keyspace));
    vtConf.setInputQuery(checkNotNull(query));
    vtConf.setSplitColumns(checkNotNull(splitColumns));
    vtConf.setSplitCount(splitCount);
    vtConf.setNumRowsPerQueryPart(numRowsPerQueryPart);
    vtConf.setAlgorithm(checkNotNull(algorithm));
    vtConf.setRpcFactoryClass(checkNotNull(rpcFactoryClass));
  }
}
