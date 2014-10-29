package com.youtube.vitess.gorpc.codecs.bson;

import com.google.common.primitives.UnsignedLong;

import org.bson.BasicBSONEncoder;

/**
 * GoRpcBsonEncoder extends BasicBSONEncoder by adding support for {@link UnsignedLong}
 */
public class GoRpcBsonEncoder extends BasicBSONEncoder {
  public static final Byte UNSIGNED_LONG = Byte.valueOf((byte) 63);

  @Override
  protected void putNumber(String name, Number n) {
    if (n instanceof UnsignedLong) {
      _put(UNSIGNED_LONG, name);
      _buf.writeLong(n.longValue());
    } else {
      super.putNumber(name, n);
    }
  }
}
