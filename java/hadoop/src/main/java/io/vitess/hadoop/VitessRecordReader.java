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

import io.vitess.client.Context;
import io.vitess.client.RpcClient;
import io.vitess.client.RpcClientFactory;
import io.vitess.client.VTGateBlockingConn;
import io.vitess.client.cursor.Cursor;
import io.vitess.client.cursor.Row;
import io.vitess.proto.Query;
import io.vitess.proto.Query.BoundQuery;
import io.vitess.proto.Topodata.TabletType;
import io.vitess.proto.Vtgate.SplitQueryResponse;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import org.joda.time.Duration;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

public class VitessRecordReader extends RecordReader<NullWritable, RowWritable> {

  private VitessInputSplit split;
  private VTGateBlockingConn vtgate;
  private VitessConf conf;
  private long rowsProcessed = 0;
  private Cursor cursor;
  private RowWritable currentRow;
  private Query.ExecuteOptions.IncludedFields includedFields;

  /**
   * Fetch connection parameters from Configuration and open VtGate connection.
   */
  @Override
  public void initialize(InputSplit split, TaskAttemptContext context)
      throws IOException, InterruptedException {
    this.split = (VitessInputSplit) split;
    conf = new VitessConf(context.getConfiguration());
    try {
      @SuppressWarnings("unchecked")
      Class<? extends RpcClientFactory> rpcFactoryClass =
          (Class<? extends RpcClientFactory>) Class.forName(conf.getRpcFactoryClass());
      List<String> addressList = Arrays.asList(conf.getHosts().split(","));
      int index = new Random().nextInt(addressList.size());

      RpcClient rpcClient = rpcFactoryClass.newInstance().create(
          Context.getDefault().withDeadlineAfter(Duration.millis(conf.getTimeoutMs())),
          addressList.get(index));
      vtgate = new VTGateBlockingConn(rpcClient);
      includedFields = conf.getIncludedFields();
    } catch (ClassNotFoundException | InstantiationException | IllegalAccessException exc) {
      throw new RuntimeException(exc);
    }
  }

  @Override
  public void close() throws IOException {
    if (vtgate != null) {
      try {
        vtgate.close();
        vtgate = null;
      } catch (IOException exc) {
        throw new RuntimeException(exc);
      }
    }
  }

  @Override
  public NullWritable getCurrentKey() throws IOException, InterruptedException {
    return NullWritable.get();
  }

  @Override
  public RowWritable getCurrentValue() throws IOException, InterruptedException {
    return currentRow;
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
    try {
      if (cursor == null) {
        SplitQueryResponse.Part splitInfo = split.getSplit();
        if (splitInfo.hasKeyRangePart()) {
          BoundQuery query = splitInfo.getQuery();
          SplitQueryResponse.KeyRangePart keyRangePart = splitInfo.getKeyRangePart();
          cursor = vtgate.streamExecuteKeyRanges(Context.getDefault(), query.getSql(),
              keyRangePart.getKeyspace(), keyRangePart.getKeyRangesList(), query.getBindVariables(),
              TabletType.RDONLY, includedFields);
        } else if (splitInfo.hasShardPart()) {
          BoundQuery query = splitInfo.getQuery();
          SplitQueryResponse.ShardPart shardPart = splitInfo.getShardPart();
          cursor = vtgate.streamExecuteShards(Context.getDefault(), query.getSql(),
              shardPart.getKeyspace(), shardPart.getShardsList(), query.getBindVariables(),
              TabletType.RDONLY, includedFields);
        } else {
          throw new IllegalArgumentException("unknown split info: " + splitInfo);
        }
      }
      Row row = cursor.next();
      if (row == null) {
        currentRow = null;
      } else {
        currentRow = new RowWritable(row);
      }
    } catch (SQLException exc) {
      throw new RuntimeException(exc);
    }

    if (currentRow == null) {
      return false;
    }
    rowsProcessed++;
    return true;
  }
}
