package refit.message;

import java.nio.ByteBuffer;
import java.security.MessageDigest;

import refit.message.REFITRequest.RequestMode;
import refit.util.REFITAssert;
import refit.util.REFITPayload;


public final class REFITReply extends REFITReplyBase {

	private static final int REPLY_HEADER_SIZE = Byte.BYTES + Short.BYTES + Integer.BYTES + Long.BYTES + Short.BYTES;

	public final boolean isFullReply;
	public final boolean isInternalOnly;
	public final long sequenceNumber;
	private ByteBuffer payload;
	public final RequestMode mode;
	public final short groupID;
	public final int viewID;

	public REFITReply(REFITUniqueID uid, short from, short groupID, int viewID, long sequenceNumber, boolean isFullReply, ByteBuffer payload, RequestMode mode, short replyReplica) {
		super(REFITMessageType.REPLY, uid, from, replyReplica);
		this.isFullReply = isFullReply;
		this.mode = mode;
		if (!isFullReply) {
			byte[] hash = new byte[payload.remaining()];
			payload.mark();
			payload.get(hash);
			payload.reset();
			payloadHash = hash;
		}
		this.payload = payload;
		this.sendHashReply = false;
		this.groupID = groupID;
		this.viewID = viewID;
		this.sequenceNumber = sequenceNumber;
		this.isInternalOnly = false;
	}

	public REFITReply(REFITUniqueID uid, short from, short groupID, RequestMode mode) {
		super(REFITMessageType.REPLY, uid, from, (short) -1);
		this.isFullReply = false;
		this.sendHashReply = false;
		this.isInternalOnly = true;
		this.groupID = groupID;
		this.viewID = -1;
		this.sequenceNumber = -1;
		this.payload = null;
		this.mode = mode;
	}

	// Used to create rejects for a reply
	public REFITReply(REFITRequest request, short from, short groupID) {
		super(REFITMessageType.REPLY, request.uid, from, (short) -1);
		this.isFullReply = false;
		this.isInternalOnly = false;
		this.groupID = groupID;
		this.viewID = -1;
		this.sequenceNumber = -1;
		this.payload = ByteBuffer.allocate(0);
		this.payloadHash = new byte[0];
		this.mode = request.mode;
	}

	public REFITReply(ByteBuffer buffer) {
		super(REFITMessageType.REPLY, buffer);
		byte flags = buffer.get();
		this.isFullReply = ((flags & 1) != 0);
		int modeIdx = (flags >> 2) & 0xf;
		this.mode = RequestMode.values()[modeIdx];
		this.groupID = buffer.getShort();
		this.viewID = buffer.getInt();
		this.sequenceNumber = buffer.getLong();
		short payloadSize = buffer.getShort();
		this.payload = buffer.slice();
		this.payload.limit(payloadSize);
		if (!isFullReply) {
			byte[] hash = new byte[payload.remaining()];
			payload.mark();
			payload.get(hash);
			payload.reset();
			payloadHash = hash;
		}
		this.sendHashReply = false;
		this.isInternalOnly = false;
	}

	@Override
	public AuthenticationType getAuthenticationType() {
		return AuthenticationType.UNICAST_MAC;
	}

	public ByteBuffer getPayload() {
		return payload.slice();
	}

	@Override
	protected void serialize(ByteBuffer buffer) {
		super.serialize(buffer);
		int modeIdx = mode.ordinal();
		REFITAssert.assertTrue(modeIdx < 16, "Only up to 16 request modes are supported");
		byte flags = (byte) ((isFullReply ? 1 : 0) +
							 (modeIdx << 2));
		buffer.put(flags);
		buffer.putShort(groupID);
		buffer.putInt(viewID);
		buffer.putLong(sequenceNumber);
		int payloadSize = payload.remaining();
		buffer.putShort((short) payloadSize);
		// replace command ByteBuffer after serialization to store the data only once
		ByteBuffer newPayload = buffer.slice();
		buffer.put(payload);
		newPayload.limit(payloadSize);
		payload = newPayload;
	}

	@Override
	protected int getHeaderSize() {
		return (super.getHeaderSize() + REPLY_HEADER_SIZE);
	}

	@Override
	protected int calculatePayloadSize() {
		return (super.calculatePayloadSize() + payload.remaining());
	}

	@Override
	public String toString() {
		return String.format("{%s|%d|%s|%s|%d|%d|%s|%d bytes}", type, from, uid, isFullReply, groupID, sequenceNumber, mode, (payload != null) ? payload.remaining() : -1);
	}


	// ###########
	// # HASHING #
	// ###########

	public transient boolean sendHashReply;
	private transient byte[] payloadHash;


	public void sendHashReply() {
		sendHashReply = true;
	}

	public REFITReply createHashedReply() {
		return new REFITReply(uid, from, groupID, viewID, sequenceNumber, false, ByteBuffer.wrap(getPayloadHash()), mode, replyReplica);
	}

	@Override
	protected byte[] calculateHash() {
		// Prepare message header
		ByteBuffer messageHeader = getInternalBuffer();
		messageHeader.limit(getHeaderSize());

		// Make sure that payload hash has been calculated (must be ensured in advance)
		getPayloadHash();

		// Calculate hash
		MessageDigest digest = REFITPayload.getDigest();
		digest.update(messageHeader);
		digest.update(payloadHash);
		return digest.digest();
	}

	@Override
	public byte[] getPayloadHash() {
		if (payloadHash == null) {
			MessageDigest hashFunction = REFITPayload.getDigest();
			payload.mark();
			hashFunction.update(payload);
			payload.reset();
			payloadHash = hashFunction.digest();
		}
		return payloadHash;
	}

	@Override
	public boolean isFullReply() {
		return isFullReply;
	}

	@Override
	public int viewID() {
		return viewID;
	}

	@Override
	public long sequenceNumber() {
		return sequenceNumber;
	}
}
