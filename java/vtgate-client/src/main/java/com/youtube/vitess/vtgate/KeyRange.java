package com.youtube.vitess.vtgate;

import java.util.HashMap;
import java.util.Map;

public class KeyRange {
	private static final String START = "Start";
	private static final String END = "End";
	private static final byte[] NULL_BYTES = "".getBytes();

	public static final KeyRange ALL = new KeyRange(null, null);

	KeyspaceId start;
	KeyspaceId end;

	public KeyRange(KeyspaceId start, KeyspaceId end) {
		this.start = start;
		this.end = end;
	}

	public Map<String, byte[]> toMap() {
		byte[] startBytes = start == null ? NULL_BYTES : start.getBytes();
		byte[] endBytes = end == null ? NULL_BYTES : end.getBytes();
		Map<String, byte[]> map = new HashMap<>();
		map.put(START, startBytes);
		map.put(END, endBytes);
		return map;
	}
}
