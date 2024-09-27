package refit.message;

import java.nio.ByteBuffer;

public final class REFITMicro extends REFITMessage {

	private static final int HEADER_SIZE = Short.BYTES;

	public final short size;

	public REFITMicro(REFITUniqueID uid, short from, short size) {
		super(REFITMessageType.MICRO, uid, from);
		this.size = size;
	}

	public REFITMicro(ByteBuffer buffer) {
		super(REFITMessageType.MICRO, buffer);
		size = buffer.getShort();
		buffer.position(buffer.position() + fillByteCount());
	}

	@Override
	public AuthenticationType getAuthenticationType() {
		return AuthenticationType.NONE;
	}

	@Override
	public boolean equals(Object object) {
		if (this == object) return true;
		if (object == null || getClass() != object.getClass()) return false;
		if (!super.equals(object)) return false;
		REFITMicro other = (REFITMicro) object;
		return other.size == size;
	}

	@Override
	protected void serialize(ByteBuffer buffer) {
		super.serialize(buffer);
		buffer.putShort(size);

		byte[] fill = new byte[fillByteCount()];
		buffer.put(fill);
	}

	@Override
	protected int getHeaderSize() {
		return (super.getHeaderSize() + HEADER_SIZE);
	}

	private int fillByteCount() {
		return Math.max(0, size - getHeaderSize());
	}

	@Override
	protected int calculatePayloadSize() {
		return (super.calculatePayloadSize() + fillByteCount());
	}

}
