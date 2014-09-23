package com.youtube.vitess.vtgate;

import com.google.common.primitives.Longs;
import com.google.common.primitives.UnsignedLong;

/**
 * KeyspaceId can be either String or UnsignedLong. Use factory method valueOf
 * to create instances
 */
public class KeyspaceId implements Comparable<KeyspaceId> {
	private final Object id;
	private final byte[] bytes;

	private KeyspaceId(Object id, byte[] bytes) {
		this.id = id;
		this.bytes = bytes;
	}

	public Object getId() {
		return id;
	}

	public byte[] getBytes() {
		return bytes;
	}

	/**
	 * Creates a KeyspaceId from id which must be a String or UnsignedLong.
	 */
	public static KeyspaceId valueOf(Object id) {
		if (id instanceof String) {
			String idStr = (String) id;
			return new KeyspaceId(idStr, idStr.getBytes());
		}

		if (id instanceof UnsignedLong) {
			UnsignedLong idULong = (UnsignedLong) id;
			return new KeyspaceId(idULong,
					Longs.toByteArray(idULong.longValue()));
		}

		throw new IllegalArgumentException(
				"invalid id type, must be either String or UnsignedLong "
						+ id.getClass());
	}

	@Override
	public int compareTo(KeyspaceId o) {
		if (o == null) {
			throw new NullPointerException();
		}

		if (id instanceof UnsignedLong && o.id instanceof UnsignedLong) {
			UnsignedLong thisId = (UnsignedLong) id;
			UnsignedLong otherId = (UnsignedLong) o.id;
			return thisId.compareTo(otherId);
		}

		if (id instanceof String && o.id instanceof String) {
			String thisId = (String) id;
			String otherId = (String) o.id;
			return thisId.compareTo(otherId);
		}

		throw new IllegalArgumentException("unexpected id types");
	}
}
