package refit.application;

import java.nio.ByteBuffer;
import java.util.Set;

import refit.message.REFITUniqueID;
import refit.replica.checkpoint.REFITCheckpointObject;

public class REFITNopSystemServer implements REFITSystemApplicationServer {

	public REFITNopSystemServer() {
	}

	@Override
	public void init() {
		// nothing to do as no state exists
	}

	@Override
	public boolean checkReadOnly(ByteBuffer payload) {
		return false;
	}

	@Override
	public REFITResult processRequest(REFITUniqueID uid, long agreementSeqNr, ByteBuffer payload) {
		return new REFITResult(ByteBuffer.wrap(new byte[0]));
	}

	@Override
	public REFITLeaderRecommendation queryLeader(int lastLeader, long targetProbeCounter) {
		// No recommendation
		return null;
	}

	@Override
	public REFITCheckpointObject[] createCheckpoint() {
		REFITCheckpointObject[] objects = new REFITCheckpointObject[1];
		objects[0] = new REFITCheckpointObject(new byte[0]);
		return objects;
	}

	@Override
	public void applyCheckpoint(REFITCheckpointObject[] checkpoint) {
		// nothing to do as no state exists
	}

}
