package refit.message;

import java.nio.ByteBuffer;

import refit.agreement.idem.IDEMMessage.IDEMCheckpointMessage;
import refit.agreement.idem.IDEMMessage.IDEMCheckpointRequest;
import refit.agreement.idem.IDEMMessage.IDEMCommit;
import refit.agreement.idem.IDEMMessage.IDEMPropose;
import refit.agreement.idem.IDEMMessage.IDEMReject;
import refit.agreement.idem.IDEMMessage.IDEMRequire;
import refit.agreement.idem.IDEMMessage.IDEMForwardRequest;
import refit.agreement.idem.IDEMMessage.IDEMViewChangeMessage;
import refit.replica.checkpoint.REFITCheckpoint;
import refit.replica.checkpoint.REFITCheckpointRequest;
import refit.replica.checkpoint.REFITCheckpointState;
import refit.util.REFITLogger;


public enum REFITMessageType {
	// NodeID exchange, must only be deserialized manually!
	CONNECT,

	// General types
	REQUEST,
	REQUEST_NO_OP,
	REQUEST_WRAPPER,
	BATCH,
	ACK_REPLY,
	REPLY,

	CHECKPOINT,
	CHECKPOINT_STATE,
	CHECKPOINT_REQUEST,

	// Idem types
	IDEM_REJECT,
	IDEM_REQUIRE,
	IDEM_FORWARD_REQUEST,
	IDEM_PROPOSE,
	IDEM_COMMIT,

	IDEM_VIEW_CHANGE,
	IDEM_CHECKPOINT_REQUEST,
	IDEM_CHECKPOINT,

	MICRO,
	;

	public static final REFITMessageType[] values = values();

	public static REFITMessageType getType(byte magic) {
		return values[magic];
	}

	public byte getMagic() {
		return (byte) ordinal();
	}

	public REFITMessage createMessage(ByteBuffer buffer) {
		switch (this) {
		case REQUEST:
			return new REFITRequest(buffer);
		case REQUEST_NO_OP:
			return new REFITNoOpRequest(buffer);
		case ACK_REPLY:
			return new REFITAckReply(buffer);
		case REPLY:
			return new REFITReply(buffer);
		case CHECKPOINT:
			return new REFITCheckpoint(buffer);
		case CHECKPOINT_STATE:
			return new REFITCheckpointState(buffer);
		case CHECKPOINT_REQUEST:
			return new REFITCheckpointRequest(buffer);
		case BATCH:
			return new REFITBatch(buffer);
		case IDEM_REJECT:
			return new IDEMReject(buffer);
		case IDEM_REQUIRE:
			return new IDEMRequire(buffer);
		case IDEM_FORWARD_REQUEST:
			return new IDEMForwardRequest(buffer);
		case IDEM_PROPOSE:
			return new IDEMPropose(buffer);
		case IDEM_COMMIT:
			return new IDEMCommit(buffer);
		case IDEM_VIEW_CHANGE:
			return new IDEMViewChangeMessage(buffer);
		case IDEM_CHECKPOINT_REQUEST:
			return new IDEMCheckpointRequest(buffer);
		case IDEM_CHECKPOINT:
			return new IDEMCheckpointMessage(buffer);
		case MICRO:
			return new REFITMicro(buffer);
		default:
			// internal and abstract message types are not listed as they must not be sent over network!
			REFITLogger.logError(REFITMessageType.class.getSimpleName(), "cannot create a message object for type " + this);
			return null;
		}
	}

}
