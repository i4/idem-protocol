package refit.replica.checkpoint;

import java.nio.ByteBuffer;

import refit.message.REFITEvent;
import refit.message.REFITMessage;
import refit.message.REFITMessageType;
import refit.message.REFITUniqueID;

public abstract class REFITCheckpointMessages extends REFITMessage {
	public REFITCheckpointMessages(REFITMessageType type, REFITUniqueID uid, short from) {
		super(type, uid, from);
	}

	public REFITCheckpointMessages(REFITMessageType expectedType, ByteBuffer buffer) {
		super(expectedType, buffer);
	}

	@Override
	public Class<? extends REFITEvent> groupType() {
		return REFITCheckpointMessages.class;
	}
}
