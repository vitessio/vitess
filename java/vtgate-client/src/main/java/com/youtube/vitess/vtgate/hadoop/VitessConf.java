package com.youtube.vitess.vtgate.hadoop;

import com.youtube.vitess.vtgate.rpcclient.RpcClientFactory;
import org.apache.hadoop.conf.Configuration;

/**
 * Collection of configuration properties used for {@link VitessInputFormat}
 */
public class VitessConf {
  public static final String HOSTS = "vitess.vtgate.hosts";
  public static final String CONN_TIMEOUT_MS = "vitess.vtgate.conn_timeout_ms";
  public static final String INPUT_KEYSPACE = "vitess.vtgate.hadoop.keyspace";
  public static final String INPUT_QUERY = "vitess.vtgate.hadoop.input_query";
  public static final String SPLITS = "vitess.vtgate.hadoop.splits";
  public static final String SPLIT_COLUMN = "vitess.vtgate.hadoop.splitcolumn";
  public static final String RPC_FACTORY_CLASS = "vtgate.rpcclient.factory";
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

  public int getSplits() {
    return conf.getInt(SPLITS, 1);
  }

  public void setSplits(int splits) {
    conf.setInt(SPLITS, splits);
  }

  public String getSplitColumn() {
    return conf.get(SPLIT_COLUMN);
  }

  public void setSplitColumn(String splitColumn) {
    conf.set(SPLIT_COLUMN, splitColumn);
  }

  public String getRpcFactoryClass() { return conf.get(RPC_FACTORY_CLASS); }

  public void setRpcFactoryClass(Class<? extends RpcClientFactory> clz) {
    conf.set(RPC_FACTORY_CLASS, clz.getName());
  }
}
