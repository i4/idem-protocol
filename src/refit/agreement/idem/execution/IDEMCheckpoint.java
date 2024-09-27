package refit.agreement.idem.execution;

import java.nio.ByteBuffer;
import java.util.HashMap;

import refit.config.REFITConfig;
import refit.message.REFITRequest;
import refit.message.REFITUniqueID;
import refit.replica.checkpoint.REFITCheckpointObject;

public class IDEMCheckpoint {

	public long sqn;
	public long[] executed;

	public IDEMResult[] results;
	public REFITCheckpointObject[] state;

	// Optimization
	public HashMap<REFITUniqueID, REFITRequest> requests;

	public IDEMCheckpoint(long sqn, long[] executed, IDEMResult[] results, REFITCheckpointObject[] state, HashMap<REFITUniqueID, REFITRequest> requests) {
		this.sqn = sqn;
		this.executed = executed.clone();
		this.results = results.clone();
		this.state = state.clone();
		if (requests != null) {
			this.requests = new HashMap<REFITUniqueID, REFITRequest>(requests);
		}
	}

	public IDEMCheckpoint(long sqn, long[] executed, IDEMResult[] results, REFITCheckpointObject[] state) {
		this(sqn, executed, results, state, null);
	}

	public IDEMCheckpoint(ByteBuffer buffer) {
		this.sqn = buffer.getLong();
		// executed
		int size = buffer.getInt();
		this.executed = new long[size];
		for (int i=0; i<size; i++) {
			executed[i] = buffer.getLong();
		}
		// results
		size = buffer.getInt();
		this.results = new IDEMResult[size];
		for (int i=0; i<size; i++) {
			if(buffer.get() == 1) {
				results[i] = new IDEMResult(buffer);
			}
		}
		// requests
		byte requestsSet = buffer.get();
		if (requestsSet == 1) {
			// TODO: Optimization
			throw new RuntimeException("Not yet implemented!");
		}
		// state
		this.state = new REFITCheckpointObject[] {new REFITCheckpointObject(buffer.slice())};
	}

	public void serialize(ByteBuffer buffer) {
		buffer.putLong(sqn);
		// executed
		buffer.putInt(executed.length);
		for (long e : executed) {
			buffer.putLong(e);
		}
		// results
		buffer.putInt(results.length);
		for (IDEMResult result : results) {
			if (result == null) {
				buffer.put((byte)0);
			} else {
				buffer.put((byte)1);
				result.serialize(buffer);
			}
		}
		// requests
		if (requests == null) {
			buffer.put((byte)0);
		} else {
			buffer.put((byte)1);
			// TODO: Optimization
			throw new RuntimeException("Not yet implemented!");
		}
		// state
		buffer.put(state[0].getBuffer());
	}

	public int payloadSize() {
		int size = Long.BYTES;
		// executed
		size += Integer.BYTES;
		size += Long.BYTES * executed.length;
		// results
		size += Integer.BYTES;
		for (int i=0; i < results.length; i++) {
			size += Byte.BYTES;
			if (results[i] != null) {
				size += results[i].payloadSize();
			}
		}
		// requests
		size += Byte.BYTES;
		if (requests != null) {
			// TODO: Optimization
			throw new RuntimeException("Not yet implemented!");
		}
		// state
		size += state[0].getMessageSize();
		return size;
	}
}
