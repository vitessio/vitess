package com.youtube.vitess.vtgate.integration;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.google.common.primitives.UnsignedLong;
import com.youtube.vitess.vtgate.KeyspaceId;

public/**
 * Helper class to hold the configurations for VtGate setup used in
 * integration tests
 */
class VtGateParams {
	String keyspace_name;
	int port;
	Map<String, List<String>> shard_kid_map;
	List<KeyspaceId> kids;

	/**
	 * Return all keyspaceIds in the Keyspace
	 */
	List<KeyspaceId> getAllKeyspaceIds() {
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
	List<KeyspaceId> getKeyspaceIds(String shardName) {
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