package refit.message;

import java.nio.ByteBuffer;


public final class REFITAckReply extends REFITReplyBase {

	private static final int REQUEST_ACK_REPLY_HEADER_SIZE = Long.SIZE;
	public final long sequenceNumber;
	public final transient short groupID;


	public REFITAckReply(REFITUniqueID uid, short from, long sequenceNumber, short groupID, short replyReplica) {
		super(REFITMessageType.ACK_REPLY, uid, from, replyReplica);
		this.sequenceNumber = sequenceNumber;
		this.groupID = groupID;
	}

	public REFITAckReply(final ByteBuffer buffer) {
		super(REFITMessageType.ACK_REPLY, buffer);
		sequenceNumber = buffer.getLong();
		groupID = -1;
	}

	@Override
	public AuthenticationType getAuthenticationType() {
		return AuthenticationType.UNICAST_MAC;
	}

	@Override
	protected void serialize(final ByteBuffer buffer) {
		super.serialize(buffer);
		buffer.putLong(sequenceNumber);
	}

	@Override
	protected int getHeaderSize() {
		return (super.getHeaderSize() + REQUEST_ACK_REPLY_HEADER_SIZE);
	}

	@Override
	public String toString() {
		return String.format("{%s|%d|%s|%d}", type, from, uid, sequenceNumber);
	}

	@Override
	public byte[] getPayloadHash() {
		ByteBuffer bb = ByteBuffer.allocate(Long.SIZE);
		bb.putLong(sequenceNumber);
		return bb.array();
	}

	@Override
	public boolean isFullReply() {
		return true;
	}

	@Override
	public int viewID() {
		return -1;
	}

	@Override
	public long sequenceNumber() {
		return sequenceNumber;
	}
}
