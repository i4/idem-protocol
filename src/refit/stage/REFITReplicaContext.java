package refit.stage;

import refit.crypto.REFITMessageAuthentication;
import refit.message.REFITMessage;
import refit.message.REFITReplyBase;
import refit.scheduler.REFITSchedulerGroup;

public abstract class REFITReplicaContext extends REFITBoundedReplicaContext {

	protected REFITReplicaContext(short id, REFITMessageAuthentication mac, REFITSchedulerGroup group) {
		super(id, mac, group);
	}

	public abstract void sendMessageToClient(REFITReplyBase reply, short clientID);

	public abstract void replicaMulticast(REFITMessage message, boolean[] replicas);

	public abstract void replicaUnicast(REFITMessage message, short replicaID);
}
