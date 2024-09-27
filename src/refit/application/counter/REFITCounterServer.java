package refit.application.counter;

import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Set;

import refit.application.REFITApplicationServer;
import refit.application.REFITResult;
import refit.config.REFITConfig;
import refit.message.REFITUniqueID;
import refit.replica.checkpoint.REFITCheckpointObject;

public class REFITCounterServer implements REFITApplicationServer {

	private final int[] counters;


	public REFITCounterServer() {
		this.counters = new int[REFITConfig.TOTAL_NR_OF_CLIENTS];
	}


	@Override
	public void init() {
		// Do nothing
	}

	@Override
	public boolean checkReadOnly(ByteBuffer payload) {
		return payload.remaining() > 0 && payload.get(payload.position()) != 0;
	}

	@Override
	public REFITResult processRequest(REFITUniqueID uid, long agreementSeqNr, ByteBuffer payload) {
		// Create reply
		short counterID = (short) (uid.nodeID - REFITConfig.TOTAL_NR_OF_REPLICAS);
		ByteBuffer result = ByteBuffer.allocate(Math.max(REFITConfig.REPLY_SIZE, Integer.BYTES));
		boolean readOnly = checkReadOnly(payload);

		if (!readOnly) {
			++counters[counterID];
		}
		result.putInt(counters[counterID]);
		result.flip();

		// Return result
		return new REFITResult(result);
	}

	@Override
	public String toString() {
		return "CNTR";
	}

	@Override
	public REFITCheckpointObject[] createCheckpoint() {
		ByteBuffer checkpoint = ByteBuffer.allocate(Integer.BYTES * counters.length);
		for (int counter : counters) checkpoint.putInt(counter);
		return new REFITCheckpointObject[]{new REFITCheckpointObject(checkpoint.array())};
	}

	@Override
	public void applyCheckpoint(final REFITCheckpointObject[] checkpoint) {
//		REFITLogger.logDebug(this, "Apply checkpoint");
		ByteBuffer checkpointBuffer = checkpoint[0].getBuffer();
		for (int i = 0; i < counters.length; i++) counters[i] = checkpointBuffer.getInt();
	}
}
