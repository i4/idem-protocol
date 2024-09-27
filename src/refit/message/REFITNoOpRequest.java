package refit.message;

import java.nio.ByteBuffer;

public final class REFITNoOpRequest extends REFITOrderRequest {

	public static final REFITNoOpRequest INSTANCE = new REFITNoOpRequest();

	static {
		INSTANCE.serializeMessage();
	}

	public REFITNoOpRequest() {
		super(REFITMessageType.REQUEST_NO_OP, new REFITUniqueID((short) -1, -1L), (short) -1);
	}

	public REFITNoOpRequest(final ByteBuffer buffer) {
		super(REFITMessageType.REQUEST_NO_OP, buffer);
	}

	@Override
	public AuthenticationType getAuthenticationType() {
		return AuthenticationType.NONE;
	}

	@Override
	protected byte[] calculateHash() {
		return new byte[0];
	}

	@Override
	public boolean equals(Object object) {
		if (this == object) return true;
		if (object == null || getClass() != object.getClass()) return false;
		return super.equals(object);
	}
}
