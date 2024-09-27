package refit.message;

import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.Collection;
import java.util.Objects;

import refit.util.REFITPayload;

public final class REFITBatch extends REFITOrderRequest {

	private static final short BATCH_ID_MAGIC = -47;
	private static final int HEADER_SIZE = Short.BYTES;
	private static final int ENTRY_HEADER_SIZE = Short.BYTES + Byte.BYTES;

	public final REFITRequest[] requests;
	public final byte[][] requestHashes;
	public final REFITUniqueID[] requestTimestamps;

	public REFITBatch(long instanceID, short from, Collection<REFITRequest> requestCollection) {
		super(REFITMessageType.BATCH, new REFITUniqueID(BATCH_ID_MAGIC, instanceID), from);
		this.requests = requestCollection.toArray(new REFITRequest[0]);

		this.requestHashes = new byte[requests.length][];
		this.requestTimestamps = new REFITUniqueID[requests.length];
		for (int i = 0; i < requests.length; i++) {
			REFITRequest request = this.requests[i];
			requestTimestamps[i] = request.uid;
		}
	}

	private REFITBatch(REFITUniqueID uid, short from, REFITRequest[] requests, byte[][] requestHashes,
	                   short[] requestGroups, REFITUniqueID[] requestTimestamps) {
		super(REFITMessageType.BATCH, uid, from);
		this.requests = requests;
		this.requestHashes = requestHashes;
		this.requestTimestamps = requestTimestamps;
	}

	public REFITBatch(ByteBuffer buffer) {
		super(REFITMessageType.BATCH, buffer);
		short batchSize = buffer.getShort();
		this.requests = new REFITRequest[batchSize];
		this.requestHashes = new byte[requests.length][];
		this.requestTimestamps = new REFITUniqueID[requests.length];
		for (int i = 0; i < batchSize; i++) {
			boolean hashOnly = buffer.get() != 0;
			if (!hashOnly) {
				REFITRequest request = new REFITRequest(buffer.slice());
				requests[i] = request;
				buffer.position(buffer.position() + request.getMessageSize());
				requestTimestamps[i] = request.uid;
			} else {
				byte[] hash = new byte[REFITPayload.HASH_SIZE];
				buffer.get(hash);
				requestHashes[i] = hash;
				requestTimestamps[i] = new REFITUniqueID(buffer.getShort(), buffer.getLong());
			}
		}
	}

	@Override
	public AuthenticationType getAuthenticationType() {
		return AuthenticationType.NONE;
	}

	@Override
	public String toString() {
		return String.format("{%s|%d|%s|%d}", type, from, uid, requests.length);
	}

	@Override
	public boolean equals(Object object) {
		if (this == object) return true;
		if (object == null || getClass() != object.getClass()) return false;
		if (!super.equals(object)) return false;
		REFITBatch other = (REFITBatch) object;
		if (requests.length != other.requests.length) return false;
		for (int i = 0; i < requests.length; i++) {
			if (!Objects.equals(requests[i], other.requests[i])) return false;
			if (!Arrays.equals(requestHashes[i], other.requestHashes[i])) return false;
			if (!Objects.equals(requestTimestamps[i], other.requestTimestamps[i])) return false;
		}
		return true;
	}


	// #################
	// # SERIALIZATION #
	// #################

	@Override
	protected void serialize(ByteBuffer buffer) {
		super.serialize(buffer);
		buffer.putShort((short) requests.length);
		for (int i = 0; i < requests.length; i++) {
			if (requests[i] != null) {
				buffer.put((byte) 0);
				requests[i].relocateTo(buffer);
			} else {
				buffer.put((byte) 1);
				buffer.put(requestHashes[i]);
				REFITUniqueID uid = requestTimestamps[i];
				buffer.putShort(uid.nodeID);
				buffer.putLong(uid.seqNr);
			}
		}
	}

	@Override
	protected int getHeaderSize() {
		return super.getHeaderSize() + HEADER_SIZE;
	}

	@Override
	protected int calculatePayloadSize() {
		int size = super.calculatePayloadSize();
		for (REFITRequest request : requests) {
			if (request == null) continue;
			size += ENTRY_HEADER_SIZE + request.calculateMessageSize();
		}
		for (byte[] requestHash : requestHashes) {
			if (requestHash == null) continue;
			size += ENTRY_HEADER_SIZE + requestHash.length + Short.BYTES + Long.BYTES;
		}
		return size;
	}


	// ###########
	// # HASHING #
	// ###########

	@Override
	protected byte[] calculateHash() {
		// Prepare batch header (= request header + batch-size field)
		ByteBuffer batchHeader = getBuffer();
		// Skip size field
		batchHeader.position(REFITBaseMessage.HEADER_SIZE);
		batchHeader.limit(getHeaderSize());

		// Make sure that message hashes have been calculated
		for (REFITRequest request : requests) {
			if (request == null) continue;
			request.getHash();
		}

		// Calculate hash
		MessageDigest digest = REFITPayload.getDigest();
		digest.update(batchHeader);

		ByteBuffer uidBuffer = ByteBuffer.allocate(Short.BYTES + Long.BYTES);
		for (int i = 0; i < requests.length; i++) {
			REFITRequest request = requests[i];
			if (request != null) {
				digest.update(request.getHash());
			} else {
				digest.update(requestHashes[i]);
			}

			REFITUniqueID uid = requestTimestamps[i];
			uidBuffer.position(0);
			uidBuffer.putShort(uid.nodeID);
			uidBuffer.putLong(uid.seqNr);
			digest.update(uidBuffer);
		}
		return digest.digest();
	}
}
