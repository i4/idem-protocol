package refit.stage;

import refit.scheduler.REFITSchedulerGroup;
import refit.scheduler.REFITSchedulerTaskType;

public abstract class REFITStage extends REFITBoundedStage {
	// shadow REFITBoundedReplicaContext variable for easier usage
	protected final REFITReplicaContext replica;

	public REFITStage(REFITSchedulerTaskType taskType, REFITReplicaContext replica) {
		super(taskType, replica);
		this.replica = replica;
	}

	public REFITStage(REFITSchedulerTaskType taskType, REFITSchedulerGroup group) {
		super(taskType, group);
		this.replica = null;
	}
}
