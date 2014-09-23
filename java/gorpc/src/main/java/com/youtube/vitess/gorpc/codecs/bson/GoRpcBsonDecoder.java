package com.youtube.vitess.gorpc.codecs.bson;

import java.io.IOException;
import java.io.InputStream;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.bson.BSONObject;
import org.bson.BasicBSONCallback;
import org.bson.ExtendableBsonDecoder;

import com.google.common.primitives.UnsignedLong;

/**
 * Modified to handle {@link UnsignedLong} type correctly
 * 
 * @see #decodeElement()
 */
public class GoRpcBsonDecoder extends ExtendableBsonDecoder {
	/**
	 * {@code 0x3f} is a valid type not supported by
	 * {@link org.bson.BasicBSONDecoder} but
	 * {@link org.bson.BasicBSONDecoder#decodeElement()} is package private and
	 * deep inside.
	 *
	 * We catch the error and handle it rather than duplicating the
	 * implementation.
	 */
	@SuppressWarnings("deprecation")
	@Override
	protected boolean decodeElement() throws IOException {
		try {
			return super.decodeElement();
		} catch (UnsupportedOperationException e) {
			Matcher matcher = Pattern.compile(
					"\\btype\\s*:\\s*([^\\s]+).*\\bname\\s*:\\s*([^\\s]+)")
					.matcher(e.getMessage());
			if (matcher.find()) {
				if (GoRpcBsonEncoder.UNSIGNED_LONG.toString().equals(
						matcher.group(1))) {
					((GoRpcBSONCallBack) _callback).gotULong(matcher.group(2),
							_in.readLong());
					return true;
				}
			}
			throw e;
		}
	}

	@Override
	public BSONObject readObject(InputStream in)
			throws IOException {
		BasicBSONCallback c = new GoRpcBSONCallBack();
		decode(in, c);
		return (BSONObject) c.get();
	}

	public static class GoRpcBSONCallBack extends BasicBSONCallback {
		public void gotULong(final String name, final long v) {
			final UnsignedLong ulv = UnsignedLong.fromLongBits(v);
			_put(name, ulv);
		}
	}
}