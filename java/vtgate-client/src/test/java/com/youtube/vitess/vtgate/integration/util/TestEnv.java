package com.youtube.vitess.vtgate.integration.util;

import com.google.common.primitives.UnsignedLong;
import com.google.gson.Gson;

import com.youtube.vitess.vtgate.KeyspaceId;

import org.apache.commons.lang.StringUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Helper class to hold the configurations for VtGate setup used in integration tests
 */
public class TestEnv {
  public Map<String, List<String>> shardKidMap;
  public Map<String, Integer> tablets;
  public String keyspace;
  public int port;
  public List<KeyspaceId> kids;

  public TestEnv(Map<String, List<String>> shardKidMap, String keyspace_name) {
    this.shardKidMap = shardKidMap;
    this.keyspace = keyspace_name;
    this.tablets = new HashMap<String, Integer>();
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
}
