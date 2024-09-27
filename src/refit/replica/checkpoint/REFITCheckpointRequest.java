package refit.replica.checkpoint;

import java.nio.ByteBuffer;

import refit.message.REFITMessageType;
import refit.message.REFITUniqueID;
import refit.util.REFITPayload;

import static refit.replica.checkpoint.REFITCheckpoint.REGULAR_CHECKPOINT_MAGIC;

public class REFITCheckpointRequest extends REFITCheckpointMessages {

	public final byte[] contentHash;

	public REFITCheckpointRequest(long checkpointCtr, short from, byte[] contentHash) {
		super(REFITMessageType.CHECKPOINT_REQUEST, new REFITUniqueID(REGULAR_CHECKPOINT_MAGIC, checkpointCtr), from);
		this.contentHash = contentHash;
		if (contentHash.length != REFITPayload.HASH_SIZE) {
			throw new AssertionError("content hash length differs from payload length");
		}
	}

	public REFITCheckpointRequest(ByteBuffer buffer) {
		super(REFITMessageType.CHECKPOINT_REQUEST, buffer);
		contentHash = new byte[REFITPayload.HASH_SIZE];
		buffer.get(contentHash);
	}

	@Override
	public AuthenticationType getAuthenticationType() {
		return AuthenticationType.UNICAST_MAC;
	}

	@Override
	public String toString() {
		return String.format("{%s|%d|%d}", type, from, uid.seqNr);
	}

	@Override
	protected void serialize(ByteBuffer buffer) {
		super.serialize(buffer);
		buffer.put(contentHash, 0, contentHash.length);
	}

	@Override
	protected int getHeaderSize() {
		return super.getHeaderSize() + REFITPayload.HASH_SIZE;
	}
}
