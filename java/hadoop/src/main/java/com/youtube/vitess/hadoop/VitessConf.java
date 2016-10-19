package com.youtube.vitess.hadoop;

import com.youtube.vitess.client.RpcClientFactory;
import com.youtube.vitess.proto.Query.SplitQueryRequest;

import java.util.Collection;

import org.apache.hadoop.conf.Configuration;

/**
 * Collection of configuration properties used for {@link VitessInputFormat}
 */
public class VitessConf {
  public static final String HOSTS = "vitess.client.hosts";
  public static final String CONN_TIMEOUT_MS = "vitess.client.conn_timeout_ms";
  public static final String INPUT_KEYSPACE = "vitess.client.keyspace";
  public static final String INPUT_QUERY = "vitess.client.input_query";
  public static final String SPLIT_COUNT = "vitess.client.split_count";
  public static final String NUM_ROWS_PER_QUERY_PART = "vitess.client.num_rows_per_query_part";
  public static final String SPLIT_COLUMNS = "vitess.client.split_columns";
  public static final String ALGORITHM = "vitess.client.algorithm";
  public static final String RPC_FACTORY_CLASS = "vitess.client.factory";
  public static final String HOSTS_DELIM = ",";

  private Configuration conf;

  public VitessConf(Configuration conf) {
    this.conf = conf;
  }

  public String getHosts() {
    return conf.get(HOSTS);
  }

  public void setHosts(String hosts) {
    conf.set(HOSTS, hosts);
  }

  public int getTimeoutMs() {
    return conf.getInt(CONN_TIMEOUT_MS, 0);
  }

  public void setTimeoutMs(int timeoutMs) {
    conf.setInt(CONN_TIMEOUT_MS, timeoutMs);
  }

  public String getKeyspace() {
    return conf.get(INPUT_KEYSPACE);
  }

  public void setKeyspace(String keyspace) {
    conf.set(INPUT_KEYSPACE, keyspace);
  }

  public String getInputQuery() {
    return conf.get(INPUT_QUERY);
  }

  public void setInputQuery(String query) {
    conf.set(INPUT_QUERY, query);
  }

  public int getSplitCount() {
    return conf.getInt(SPLIT_COUNT, 0);
  }

  public void setSplitCount(int splits) {
    conf.setInt(SPLIT_COUNT, splits);
  }

  public Collection<String> getSplitColumns() {
    return conf.getStringCollection(SPLIT_COLUMNS);
  }

  public void setSplitColumns(Collection<String> splitColumns) {
    conf.setStrings(SPLIT_COLUMNS, splitColumns.toArray(new String[0]));
  }

  public int getNumRowsPerQueryPart() {
    return conf.getInt(NUM_ROWS_PER_QUERY_PART, 100000);
  }

  public void setNumRowsPerQueryPart(int numRowsPerQueryPart) {
    conf.setInt(NUM_ROWS_PER_QUERY_PART, numRowsPerQueryPart);
  }

  public SplitQueryRequest.Algorithm getAlgorithm() {
    return conf.getEnum(ALGORITHM, SplitQueryRequest.Algorithm.EQUAL_SPLITS);
  }

  public void setAlgorithm(SplitQueryRequest.Algorithm algorithm) {
    conf.setEnum(ALGORITHM, algorithm);
  }

  public String getRpcFactoryClass() { return conf.get(RPC_FACTORY_CLASS); }

  public void setRpcFactoryClass(Class<? extends RpcClientFactory> clz) {
    conf.set(RPC_FACTORY_CLASS, clz.getName());
  }
}
