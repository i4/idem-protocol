package refit.message;

import java.nio.ByteBuffer;

public abstract class REFITReplyBase extends REFITMessage {
	public final transient short replyReplica;

	public REFITReplyBase(REFITMessageType type, REFITUniqueID uid, short from, short replyReplica) {
		super(type, uid, from);
		this.replyReplica = replyReplica;
	}

	public REFITReplyBase(REFITMessageType expectedType, ByteBuffer buffer) {
		super(expectedType, buffer);
		this.replyReplica = -1;
	}

	public abstract byte[] getPayloadHash();

	public abstract boolean isFullReply();

	public abstract int viewID();

	public abstract long sequenceNumber();
}
