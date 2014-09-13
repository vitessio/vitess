package org.bson;

import java.io.IOException;

/**
 * Increases visibility of {@link #decodeElement}.
 */
public class ExtendableBsonDecoder extends BasicBSONDecoder {
	@Override
	protected boolean decodeElement() throws IOException {
		return super.decodeElement();
	}
}