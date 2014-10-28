package com.youtube.vitess.vtgate;

import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;

import com.google.common.primitives.Longs;
import com.google.common.primitives.UnsignedLong;

/**
 * KeyspaceId can be either String or UnsignedLong. Use factory method valueOf to create instances
 */
public class KeyspaceId implements Comparable<KeyspaceId> {
  private Object id;

  public static final String COL_NAME = "keyspace_id";

  public KeyspaceId() {

  }

  public void setId(Object id) {
    if (!(id instanceof String) && !(id instanceof UnsignedLong)) {
      throw new IllegalArgumentException("invalid id type, must be either String or UnsignedLong "
          + id.getClass());
    }

    this.id = id;
  }

  public Object getId() {
    return id;
  }

  public byte[] getBytes() {
    if (id instanceof String) {
      try {
        return Hex.decodeHex(((String) id).toCharArray());
      } catch (DecoderException e) {
        throw new IllegalArgumentException("illegal string id", e);
      }
    } else {
      return Longs.toByteArray(((UnsignedLong) id).longValue());
    }
  }

  /**
   * Creates a KeyspaceId from id which must be a String or UnsignedLong.
   */
  public static KeyspaceId valueOf(Object id) {
    KeyspaceId kid = new KeyspaceId();
    kid.setId(id);
    return kid;
  }

  @Override
  public boolean equals(Object o) {
    if (o instanceof KeyspaceId) {
      return this.compareTo((KeyspaceId) o) == 0;
    }
    return false;
  }

  @Override
  public int hashCode() {
    if (id instanceof UnsignedLong) {
      return ((UnsignedLong) id).hashCode();
    }
    return ((String) id).hashCode();
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
