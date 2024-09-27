package refit.message;

import java.nio.ByteBuffer;

public final class REFITConnect extends REFITMessage {
	// client
	public static final long CONNECT_SEQNR = -1;
	// server
	public static final long ACCEPT_SEQNR = -2;

	public static final int MSG_SIZE;

	static {
		REFITConnect connect = new REFITConnect(new REFITUniqueID((short) 0, 0), (short) 0);
		MSG_SIZE = connect.calculateMessageSize();
	}

	public REFITConnect(REFITUniqueID uid, short from) {
		super(REFITMessageType.CONNECT, uid, from);
	}

	public REFITConnect(ByteBuffer buffer) {
		super(REFITMessageType.CONNECT, buffer);
	}

	@Override
	public AuthenticationType getAuthenticationType() {
		return AuthenticationType.NONE;
	}
}
