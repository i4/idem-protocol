package refit.application.echo;

import java.nio.ByteBuffer;
import java.util.Set;

import refit.application.REFITApplicationServer;
import refit.application.REFITResult;
import refit.message.REFITUniqueID;
import refit.replica.checkpoint.REFITCheckpointObject;

public class REFITEchoServer implements REFITApplicationServer {

	@Override
	public void init() {
		// nop
	}

	@Override
	public boolean checkReadOnly(ByteBuffer payload) {
		return false;
	}

	@Override
	public REFITResult processRequest(REFITUniqueID uid, long agreementSeqNr, ByteBuffer payload) {
		return new REFITResult(payload);
	}

	@Override
	public REFITCheckpointObject[] createCheckpoint() {
		return new REFITCheckpointObject[0];
	}

	@Override
	public void applyCheckpoint(REFITCheckpointObject[] checkpoint) {
		// nop
	}
}
