package refit.communication;

import refit.scheduler.REFITSchedulerGroup;

public interface REFITScheduledRouter {
	REFITSchedulerGroup getGroup();

	short getNodeID();
}
