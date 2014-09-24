package com.youtube.vitess.vtgate.integration.util;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.google.common.primitives.UnsignedLong;
import com.youtube.vitess.vtgate.KeyspaceId;

/**
 * Helper class to hold the configurations for VtGate setup used in integration
 * tests
 */
public class VtGateParams {
	public String keyspace_name;
	public int port;
	public Map<String, List<String>> shard_kid_map;
	public List<KeyspaceId> kids;

	/**
	 * Return all keyspaceIds in the Keyspace
	 */
	public List<KeyspaceId> getAllKeyspaceIds() {
		if (kids != null) {
			return kids;
		}

		kids = new ArrayList<>();
		for (List<String> ids : shard_kid_map.values()) {
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
		List<String> kidsStr = shard_kid_map.get(shardName);
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