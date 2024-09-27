package refit.replica.checkpoint;

import java.nio.ByteBuffer;
import java.security.MessageDigest;

import refit.message.REFITMessageType;
import refit.message.REFITUniqueID;
import refit.util.REFITPayload;

import static refit.replica.checkpoint.REFITCheckpoint.REGULAR_CHECKPOINT_MAGIC;


public final class REFITCheckpointState extends REFITCheckpointMessages {

	public final long execCtr;
	public final long agreementProgress;
	public final REFITCheckpointObject[] data;
	private transient byte[] contentHash;


	public REFITCheckpointState(long checkpointCtr, short from, long execCtr, long agreementProgress, boolean isLightweight, REFITCheckpointObject[] data) {
		super(REFITMessageType.CHECKPOINT_STATE, new REFITUniqueID(REGULAR_CHECKPOINT_MAGIC, checkpointCtr), from);
		this.execCtr = execCtr;
		this.agreementProgress = agreementProgress;
		this.data = new REFITCheckpointObject[data.length];
		System.arraycopy(data, 0, this.data, 0, data.length);
	}

	public REFITCheckpointState(ByteBuffer buffer) {
		super(REFITMessageType.CHECKPOINT_STATE, buffer);
		execCtr = buffer.getLong();
		agreementProgress = buffer.getLong();
		int objectCount = buffer.getInt();
		this.data = new REFITCheckpointObject[objectCount];
		for (int i = 0; i < objectCount; i++) {
			final int size = buffer.getInt();
			int oldLimit = buffer.limit();
			buffer.limit(buffer.position() + size);
			ByteBuffer slice = buffer.slice();
			// the REFITObjects may later be converted to the correct type
			data[i] = new REFITCheckpointObject(slice);
			buffer.limit(oldLimit);
			buffer.position(buffer.position() + size);
		}
	}

	@Override
	public AuthenticationType getAuthenticationType() {
		return AuthenticationType.NONE;
	}

	@Override
	public String toString() {
		return String.format("{%s|%d|%d|%d|%d|%d}", type, from, uid.seqNr, execCtr, agreementProgress, data.length);
	}

	@Override
	protected void serialize(ByteBuffer buffer) {
		super.serialize(buffer);
		buffer.putLong(execCtr);
		buffer.putLong(agreementProgress);
		buffer.putInt(data.length);
		for (REFITCheckpointObject object : data) {
			buffer.putInt(object.getMessageSize());
			buffer.put(object.getBuffer());
		}
	}

	@Override
	protected int calculatePayloadSize() {
		int size = super.calculatePayloadSize();
		size += Integer.BYTES + Long.BYTES + Long.BYTES;
		for (REFITCheckpointObject object : data) {
			object.serializeMessage();
			size += Integer.BYTES + object.getMessageSize();
		}

		return size;
	}

	public byte[] getContentHash() {
		if (contentHash != null) return contentHash;

		ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
		buffer.putLong(uid.seqNr);

		// enforce hash calculation
		for (REFITCheckpointObject object : data) {
			object.getHash();
		}

		MessageDigest digest = REFITPayload.getDigest();
		digest.update(buffer);
		for (REFITCheckpointObject object : data) {
			digest.update(object.getHash());
		}
		contentHash = digest.digest();

		return contentHash;
	}

	public REFITCheckpoint toCheckpoint() {
		return new REFITCheckpoint(uid.seqNr, from, execCtr, agreementProgress, getContentHash());
	}
}
