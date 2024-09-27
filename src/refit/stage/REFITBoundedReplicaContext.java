package refit.stage;

import refit.crypto.REFITMessageAuthentication;
import refit.message.REFITMessage;
import refit.scheduler.REFITSchedulerGroup;
import refit.util.REFITDataLogger;

public abstract class REFITBoundedReplicaContext {
	public final short id;
	public final REFITMessageAuthentication mac;
	public final REFITSchedulerGroup group;
	public REFITDataLogger logger;

	protected REFITBoundedReplicaContext(short id, REFITMessageAuthentication mac, REFITSchedulerGroup group) {
		this.id = id;
		this.mac = mac;
		this.group = group;
	}

	public abstract boolean replicaBoundedUnicast(REFITMessage message, short replicaID, REFITOutbox outbox);

}
