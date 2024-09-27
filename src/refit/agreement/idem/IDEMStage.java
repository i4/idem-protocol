package refit.agreement.idem;

import refit.message.REFITMessage;
import refit.replica.REFITBaseReplica;
import refit.scheduler.REFITSchedulerTaskType;
import refit.stage.REFITObserver;
import refit.stage.REFITReplicaContext;
import refit.stage.REFITStage;

public abstract class IDEMStage extends REFITStage {

	public IDEMStage(REFITSchedulerTaskType taskType, REFITReplicaContext replica) {
		super(taskType, replica);
	}

	protected void tellAll(REFITMessage message) {
		tell(message);
		message.serializeMessage();
		message.markVerified();
		replica.replicaMulticast(message, REFITBaseReplica.ALL_REPLICAS);
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	protected void tellAll(REFITMessage message, REFITObserver observer) {
		observer.tell(message);
		message.serializeMessage();
		message.markVerified();
		replica.replicaMulticast(message, REFITBaseReplica.ALL_REPLICAS);
	}

	protected void tellOthers(REFITMessage message) {
		message.serializeMessage();
		message.markVerified();
		replica.replicaMulticast(message, REFITBaseReplica.ALL_REPLICAS);
	}
}
