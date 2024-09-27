package refit.application;

import java.nio.ByteBuffer;

import refit.message.REFITUniqueID;
import refit.replica.checkpoint.REFITCheckpointObject;

public interface REFITApplicationServer {
	void init();

	REFITResult processRequest(REFITUniqueID uid, long agreementSeqNr, ByteBuffer payload);

	boolean checkReadOnly(ByteBuffer payload);

	REFITCheckpointObject[] createCheckpoint();

	void applyCheckpoint(REFITCheckpointObject[] checkpoint);
}
