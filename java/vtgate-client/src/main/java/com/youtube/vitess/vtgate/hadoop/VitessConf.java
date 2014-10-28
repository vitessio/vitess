package com.youtube.vitess.vtgate.hadoop;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;

import java.util.Arrays;
import java.util.List;

/**
 * Collection of configuration properties used for {@link VitessInputFormat}
 */
public class VitessConf {
  public static final String HOSTS = "vitess.vtgate.hosts";
  public static final String CONN_TIMEOUT_MS = "vitess.vtgate.conn_timeout_ms";
  public static final String INPUT_KEYSPACE = "vitess.vtgate.hadoop.keyspace";
  public static final String INPUT_TABLE = "vitess.vtgate.hadoop.input_table";
  public static final String INPUT_COLUMNS = "vitess.vtgate.hadoop.input_columns";
  public static final String SPLITS_PER_SHARD = "vitess.vtgate.hadoop.splits_per_shard";
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

  public String getInputTable() {
    return conf.get(INPUT_TABLE);
  }

  public void setInputTable(String table) {
    conf.set(INPUT_TABLE, table);
  }

  public List<String> getInputColumns() {
    return Arrays.asList(StringUtils.split(conf.get(INPUT_COLUMNS), HOSTS_DELIM));
  }

  public void setInputColumns(List<String> columns) {
    conf.set(INPUT_COLUMNS, StringUtils.join(columns, HOSTS_DELIM));
  }

  public int getSplitsPerShard() {
    return conf.getInt(SPLITS_PER_SHARD, 1);
  }

  public void setSplitsPerShard(int splitsPerShard) {
    conf.setInt(SPLITS_PER_SHARD, splitsPerShard);
  }
}
