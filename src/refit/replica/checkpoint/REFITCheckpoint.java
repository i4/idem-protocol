package refit.replica.checkpoint;

import java.nio.ByteBuffer;
import java.util.Arrays;

import refit.message.REFITMessageType;
import refit.message.REFITUniqueID;
import refit.util.REFITPayload;


public final class REFITCheckpoint extends REFITCheckpointMessages {

	static final short REGULAR_CHECKPOINT_MAGIC = -1;

	public final long execCtr;
	public final long agreementProgress;
	public final byte[] contentHash;
	private transient REFITCheckpointState state;

	public REFITCheckpoint(long checkpointCtr, short from, long execCtr, long agreementProgress, byte[] contentHash) {
		super(REFITMessageType.CHECKPOINT, new REFITUniqueID(REGULAR_CHECKPOINT_MAGIC, checkpointCtr), from);
		this.execCtr = execCtr;
		this.agreementProgress = agreementProgress;
		this.contentHash = contentHash;
		if (contentHash.length != REFITPayload.HASH_SIZE) {
			throw new AssertionError("content hash length differs from payload length");
		}
	}

	public REFITCheckpoint(ByteBuffer buffer) {
		super(REFITMessageType.CHECKPOINT, buffer);
		execCtr = buffer.getLong();
		agreementProgress = buffer.getLong();
		contentHash = new byte[REFITPayload.HASH_SIZE];
		buffer.get(contentHash);
	}

	@Override
	public AuthenticationType getAuthenticationType() {
		return AuthenticationType.SIGNATURE;
	}

	@Override
	public String toString() {
		return String.format("{%s|%d|%d|%d|%d}", type, from, uid.seqNr, execCtr, agreementProgress);
	}

	@Override
	protected void serialize(ByteBuffer buffer) {
		super.serialize(buffer);
		buffer.putLong(execCtr);
		buffer.putLong(agreementProgress);
		buffer.put(contentHash, 0, contentHash.length);
	}

	@Override
	protected int getHeaderSize() {
		return super.getHeaderSize() + REFITPayload.HASH_SIZE + Long.BYTES + Long.BYTES;
	}

	public boolean hasState() {
		return (state != null);
	}

	public REFITCheckpointState getState() {
		return state;
	}

	public boolean setState(REFITCheckpointState state) {
		if (!uid.equals(state.uid)) return false;
		if (execCtr != state.execCtr) return false;
		if (state.agreementProgress != agreementProgress) return false;
		if (!Arrays.equals(state.getContentHash(), contentHash)) return false;
		this.state = state;
		return true;
	}
}
