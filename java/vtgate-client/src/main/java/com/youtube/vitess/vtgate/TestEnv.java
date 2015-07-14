package com.youtube.vitess.vtgate;

import com.google.common.primitives.UnsignedLong;
import com.google.gson.Gson;
import com.youtube.vitess.vtgate.KeyspaceId;
import com.youtube.vitess.vtgate.rpcclient.RpcClientFactory;
import org.apache.commons.lang.StringUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Helper class to hold the configurations for VtGate setup used in integration tests
 */
public class TestEnv {
  public static final String PROPERTY_KEY_RPCCLIENT_FACTORY_CLASS = "vtgate.rpcclient.factory";
  private Map<String, List<String>> shardKidMap;
  private Map<String, Integer> tablets = new HashMap<>();
  private String keyspace;
  private Process pythonScriptProcess;
  private int port;
  private List<KeyspaceId> kids;

  public void setKeyspace(String keyspace) {
    this.keyspace = keyspace;
  }

  public String getKeyspace() {
    return this.keyspace;
  }

  public int getPort() {
    return this.port;
  }

  public void setPort(int port) {
    this.port = port;
  }

  public Process getPythonScriptProcess() {
    return this.pythonScriptProcess;
  }

  public void setPythonScriptProcess(Process process) {
    this.pythonScriptProcess = process;
  }

  public void setShardKidMap(Map<String, List<String>> shardKidMap) {
    this.shardKidMap = shardKidMap;
  }

  public Map<String, List<String>> getShardKidMap() {
    return this.shardKidMap;
  }

  public void addTablet(String type, int count) {
    tablets.put(type, count);
  }

  public String getShardNames() {
    return StringUtils.join(shardKidMap.keySet(), ",");
  }

  public String getTabletConfig() {
    return new Gson().toJson(tablets);
  }

  /**
   * Return all keyspaceIds in the Keyspace
   */
  public List<KeyspaceId> getAllKeyspaceIds() {
    if (kids != null) {
      return kids;
    }

    kids = new ArrayList<>();
    for (List<String> ids : shardKidMap.values()) {
      for (String id : ids) {
        kids.add(KeyspaceId.valueOf(UnsignedLong.valueOf(id)));
      }
    }
    return kids;
  }

  /**
   * Return all keyspaceIds in a specific shard
   */
  public List<KeyspaceId> getKeyspaceIds(String shardName) {
    List<String> kidsStr = shardKidMap.get(shardName);
    if (kidsStr != null) {
      List<KeyspaceId> kids = new ArrayList<>();
      for (String kid : kidsStr) {
        kids.add(KeyspaceId.valueOf(UnsignedLong.valueOf(kid)));
      }
      return kids;
    }
    return null;
  }

  /**
   * Get setup command to launch a cluster.
   */
  public List<String> getSetupCommand() {
    String vtTop = System.getenv("VTTOP");
    if (vtTop == null) {
      throw new RuntimeException("cannot find env variable: VTTOP");
    }
    List<String> command = new ArrayList<String>();
    command.add("python");
    command.add(vtTop + "/test/java_vtgate_test_helper.py");
    command.add("--shards");
    command.add(getShardNames());
    command.add("--tablet-config");
    command.add(getTabletConfig());
    command.add("--keyspace");
    command.add(keyspace);
    return command;
  }

  public RpcClientFactory getRpcClientFactory() {
    String rpcClientFactoryClass = System.getProperty(PROPERTY_KEY_RPCCLIENT_FACTORY_CLASS);
    try {
      Class<?> clazz = Class.forName(rpcClientFactoryClass);
      return (RpcClientFactory)clazz.newInstance();
    } catch (ClassNotFoundException|IllegalAccessException|InstantiationException e) {
      throw new RuntimeException(e);
    }
  }
}
