package refit.replica.checkpoint;

import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.util.Objects;

import refit.config.REFITConfig;
import refit.util.REFITPayload;

public class REFITCheckpointObject {

	protected transient ByteBuffer serializationBuffer;
	protected int messageSize;

	protected REFITCheckpointObject() {
		// only for use by subclasses
	}

	public REFITCheckpointObject(byte[] payload) {
		this.serializationBuffer = ByteBuffer.wrap(payload);
		this.messageSize = payload.length;
	}

	public REFITCheckpointObject(final ByteBuffer buffer) {
		if (REFITConfig.ENABLE_DEBUG_CHECKS && buffer.position() != 0) {
			throw new AssertionError("The passed ByteBuffer must start at position zero!");
		}
		this.serializationBuffer = buffer;
		this.messageSize = buffer.capacity();
	}

	protected void serialize(ByteBuffer buffer) {
		throw new IllegalArgumentException("Not allowed in base class");
	}

	public int getMessageSize() {
		return messageSize;
	}

	protected int calculatePayloadSize() {
		return getMessageSize();
	}

	public ByteBuffer getBuffer() {
		if (serializationBuffer == null) {
			throw new NullPointerException("getBuffer() called on a null serialization buffer");
		}
		serializationBuffer.position(0);
		serializationBuffer.limit(messageSize);
		return serializationBuffer.slice();
	}

	public boolean serializeMessage() {
		// Serialize a message only once
		if (serializationBuffer != null) {
			return false;
		}

		// Create serialization buffer
		int messageSize = calculatePayloadSize();
		serializationBuffer = ByteBuffer.allocate(messageSize);

		// Serialize message
		serialize(serializationBuffer);
		serializationBuffer.flip();
		this.messageSize = serializationBuffer.limit();
		return true;
	}

	@Override
	public String toString() {
		return "{O}";
	}

	// ################
	// # MESSAGE HASH #
	// ################

	private transient byte[] hash;

	public byte[] getHash() {
		if (hash == null) {
			hash = calculateHash();
		}
		return hash;
	}

	protected byte[] calculateHash() {
		// Prepare message buffer
		final ByteBuffer messageBuffer = getBuffer();
		messageBuffer.limit(calculatePayloadSize());

		// Calculate hash
		final MessageDigest digest = REFITPayload.getDigest();
		REFITPayload.pauseableUpdate(digest, messageBuffer);
		return digest.digest();
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		REFITCheckpointObject that = (REFITCheckpointObject) o;
		return Objects.equals(getBuffer(), that.getBuffer());
	}

	@Override
	public int hashCode() {
		return Objects.hash(getBuffer());
	}

}
