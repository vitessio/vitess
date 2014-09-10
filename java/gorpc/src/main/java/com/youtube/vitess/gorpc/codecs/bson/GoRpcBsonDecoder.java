package com.youtube.vitess.gorpc.codecs.bson;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.bson.ExtendableBsonDecoder;

/**
 * Modified to match GoRpc bson encoder.
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
				// 0x3f is 63
				if ("63".equals(matcher.group(1))) {
					_callback.gotLong(matcher.group(2), _in.readLong());
					return true;
				}
			}
			throw e;
		}
	}
}