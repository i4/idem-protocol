package refit.message;

import java.nio.ByteBuffer;

import refit.config.REFITConfig;


public abstract class REFITBaseMessage {

	protected REFITBaseMessage() {
	}

	@Override
	public String toString() {
		return "{}";
	}


	// ###################################
	// # SERIALIZATION & DESERIALIZATION #
	// ###################################

	// Header fields :                              size
	protected static final int HEADER_SIZE = Integer.BYTES;

	private transient int messageSize = -1;
	private transient int paddingSize = -1;
	private transient ByteBuffer serializationBuffer;

	protected REFITBaseMessage(ByteBuffer buffer) {
		if (REFITConfig.ENABLE_DEBUG_CHECKS && buffer.position() != 0) {
			throw new AssertionError("The passed ByteBuffer must start at position zero!");
		}
		this.serializationBuffer = buffer;
		this.messageSize = buffer.getInt();
	}

	final public int getMessageSize() {
		return messageSize;
	}

	protected ByteBuffer getInternalBuffer() {
		if (serializationBuffer == null) {
			throw new NullPointerException("getBuffer() called on a null serialization buffer");
		}
		serializationBuffer.limit(messageSize);
		serializationBuffer.position(0);
		if (REFITConfig.ENABLE_DEBUG_CHECKS && serializationBuffer.getInt(0) != messageSize) {
			throw new AssertionError("Corrupt message");
		}
		return serializationBuffer;
	}

	public ByteBuffer getBuffer() {
		return getInternalBuffer().slice();
	}

	protected int calculatePaddingSize() {
		if (paddingSize != -1) {
			return paddingSize;
		}
		return 0;
	}

	public void setPaddingSize(int paddingSize) {
		if (this.paddingSize != -1 && paddingSize != this.paddingSize) {
			throw new IllegalStateException("padding size is already set");
		}
		this.paddingSize = paddingSize;
	}

	public int calculateMessageSize() {
		if (serializationBuffer != null) {
			return messageSize;
		} else {
			return (getHeaderSize() + calculatePayloadSize() + calculatePaddingSize());
		}
	}

	public boolean serializeMessage() {
		return serializeMessage(false);
	}

	public boolean serializeMessage(boolean force) {
		// Serialize a message only once
		if (!force && serializationBuffer != null) return false;

		// Create serialization buffer
		setPaddingSize(calculatePaddingSize());
		messageSize = calculateMessageSize();
		serializationBuffer = ByteBuffer.allocate(messageSize);

		// Serialize message
		serialize(serializationBuffer);

		return true;
	}

	public void serializeInto(final ByteBuffer target) {
		if (serializationBuffer == null) {
			// Setup serialization buffer
			setPaddingSize(calculatePaddingSize());
			messageSize = calculateMessageSize();
			serializationBuffer = target.slice();
			serializationBuffer.limit(messageSize);

			// Serialize message
			serialize(serializationBuffer);

			// Move target buffer forward
			target.position(target.position() + messageSize);
		} else {
			relocateTo(target);
		}
	}

	protected void serialize(ByteBuffer buffer) {
		buffer.putInt(messageSize);
	}

	public void relocateTo(final ByteBuffer target) {
		if (serializationBuffer == null) {
			throw new IllegalStateException("Message must already be serialized");
		}
		ByteBuffer newSerializationBuffer = target.slice();
		newSerializationBuffer.limit(messageSize);

		target.put(getInternalBuffer());

		serializationBuffer = newSerializationBuffer;
	}

	protected int getHeaderSize() {
		return HEADER_SIZE;
	}

	protected int calculatePayloadSize() {
		return 0;
	}

}
