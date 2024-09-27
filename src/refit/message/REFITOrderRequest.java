package refit.message;

import java.nio.ByteBuffer;

public abstract class REFITOrderRequest extends REFITMessage {
	public REFITOrderRequest(REFITMessageType type, REFITUniqueID uid, short from) {
		super(type, uid, from);
	}

	public REFITOrderRequest(REFITMessageType expectedType, ByteBuffer buffer) {
		super(expectedType, buffer);
	}
}
