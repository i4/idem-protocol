package refit.message;

import java.nio.ByteBuffer;

public final class REFITRequestWrapper extends REFITMessage {

	private static final int HEADER_SIZE = 0;

	private final REFITRequest request;

	public REFITRequestWrapper(REFITUniqueID uid, short from, REFITRequest request) {
		super(REFITMessageType.REQUEST_WRAPPER, uid, from);
		this.request = request;
		// call getBuffer to assert that the request is already serialized
		request.getBuffer();
	}

	public REFITRequestWrapper(ByteBuffer buffer) {
		super(REFITMessageType.REQUEST_WRAPPER, buffer);
		this.request = (REFITRequest) REFITMessage.createMessage(buffer.slice());
		buffer.position(buffer.position() + this.request.getMessageSize());
	}

	@Override
	public AuthenticationType getAuthenticationType() {
		return AuthenticationType.UNICAST_MAC;
	}

	public REFITRequest getRequest() {
		return request;
	}

	@Override
	public boolean equals(Object object) {
		if (this == object) return true;
		if (object == null || getClass() != object.getClass()) return false;
		if (!super.equals(object)) return false;
		REFITRequestWrapper other = (REFITRequestWrapper) object;
		return request.equals(other.request);
	}

	@Override
	protected void serialize(ByteBuffer buffer) {
		super.serialize(buffer);
		request.relocateTo(buffer);
	}

	@Override
	public void relocateTo(ByteBuffer target) {
		super.relocateTo(target);
		ByteBuffer buffer = getInternalBuffer();
		buffer.position(getHeaderSize());
		// actually a duplicate data copy ...
		request.relocateTo(buffer);
	}

	@Override
	protected int getHeaderSize() {
		return (super.getHeaderSize() + HEADER_SIZE);
	}

	@Override
	protected int calculatePayloadSize() {
		return (super.calculatePayloadSize() + request.calculateMessageSize());
	}
}
