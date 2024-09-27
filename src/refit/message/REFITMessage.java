package refit.message;

import java.nio.ByteBuffer;
import java.security.MessageDigest;

import refit.config.REFITConfig;
import refit.crypto.REFITMessageAuthentication;
import refit.util.REFITPayload;


public abstract class REFITMessage extends REFITBaseMessage implements REFITEvent {

	public enum AuthenticationType {
		MAC_AUTHENTICATOR,
		UNICAST_MAC,
		SIGNATURE,
		NONE,
	}

	public final REFITMessageType type;
	public final REFITUniqueID uid;
	public final short from;
	private final StackTraceElement[] ste;

	// A uid that refers to an agreement sequence number must either point at a specific request or update for that seq#
	// or in case of a checkpoint refer to the sequence number from which processing will continue
	// More general: a sequence number always points to or before a slot.
	// In a sequence number range the start seq# is included and the end seq# is excluded
	protected REFITMessage(REFITMessageType type, REFITUniqueID uid, short from) {
		// Initialize attributes
		this.type = type;
		this.uid = uid;
		this.from = from;
		this.fromInternal = true;

		ste = (REFITConfig.TRACE_MESSAGES) ? Thread.currentThread().getStackTrace() : null;
	}

	@Override
	public boolean equals(Object object) {
		if (this == object) return true;
		if (object == null || getClass() != object.getClass()) return false;
		REFITMessage other = (REFITMessage) object;
		return from == other.from &&
				type == other.type &&
				uid.equals(other.uid);
	}

	@Override
	public int hashCode() {
		return uid.hashCode();
	}

	@Override
	public String toString() {
		return String.format("{%s|%d|%s}", type, from, uid);
	}

	public StackTraceElement[] getSte() {
		return ste;
	}

	// ###################################
	// # SERIALIZATION & DESERIALIZATION #
	// ###################################

	// Header fields :                        type   +    uid.id   + uid.seqNr  +    from
	public static final int HEADER_SIZE = Byte.BYTES + Short.BYTES + Long.BYTES + Short.BYTES;
	private static final int TYPE_POSITION = Integer.BYTES;


	protected REFITMessage(REFITMessageType expectedType, ByteBuffer buffer) {
		super(buffer);
		this.type = REFITMessageType.getType(buffer.get());
		if (REFITConfig.ENABLE_DEBUG_CHECKS && type != expectedType) {
			throw new AssertionError("Mismatch between actual and expected message type. got: " + type + " expected: " + expectedType);
		}
		this.uid = new REFITUniqueID(buffer.getShort(), buffer.getLong());
		this.from = buffer.getShort();
		this.fromInternal = false;

		ste = (REFITConfig.TRACE_MESSAGES) ? Thread.currentThread().getStackTrace() : null;
	}

	@Override
	public int calculatePaddingSize() {
		switch (getAuthenticationType()) {
		case SIGNATURE:
			return REFITMessageAuthentication.SIGNATURE_SIZE;
		case UNICAST_MAC:
			return REFITMessageAuthentication.UNICAST_MAC_SIZE;
		case MAC_AUTHENTICATOR:
			int paddingSize = super.calculatePaddingSize();
			if (paddingSize <= 0) {
				return REFITMessageAuthentication.calculateMACSize(REFITConfig.TOTAL_NR_OF_REPLICAS);
			}
			return paddingSize;
		case NONE:
			return 0;
		default:
			throw new InternalError("This message is not serializable!");
		}
	}

	public static REFITMessage createMessage(ByteBuffer buffer) {
		REFITMessageType type = REFITMessageType.getType(buffer.get(buffer.position() + TYPE_POSITION));
		return type.createMessage(buffer);
	}

	@Override
	protected void serialize(ByteBuffer buffer) {
		super.serialize(buffer);
		buffer.put(type.getMagic());
		buffer.putShort(uid.nodeID);
		buffer.putLong(uid.seqNr);
		buffer.putShort(from);
	}

	@Override
	protected int getHeaderSize() {
		return super.getHeaderSize() + HEADER_SIZE;
	}

	public ByteBuffer getPaddingBuffer() {
		ByteBuffer myBuffer = getInternalBuffer();
		int paddingStart = getHeaderSize() + calculatePayloadSize();
		myBuffer.position(paddingStart);
		return myBuffer.slice();
	}

	// ##################
	// # AUTHENTICATION #
	// ##################

	private final transient boolean fromInternal;
	private transient boolean isVerified = false;

	public abstract AuthenticationType getAuthenticationType();

	public boolean isInternal() {
		return fromInternal;
	}

	public boolean isVerified() {
		return isVerified;
	}

	public void markVerified() {
		this.isVerified = true;
	}


	// ################
	// # MESSAGE HASH #
	// ################

	private transient byte[] hash;


	public byte[] getHash() {
		if (hash == null) hash = calculateHash();
		return hash;
	}

	protected byte[] calculateHash() {
		// Prepare message buffer
		ByteBuffer messageBuffer = getInternalBuffer();
		messageBuffer.limit(getHeaderSize() + calculatePayloadSize());

		// Calculate hash
		MessageDigest digest = REFITPayload.getDigest();
		digest.update(messageBuffer);
		return digest.digest();
	}
}
