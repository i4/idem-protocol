package refit.scheduler;

import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumMap;
import java.util.Map;

import refit.util.REFITLogger;

public class REFITSchedulerGroup {

	// #############################
	// # TABLE OF UNASSIGNED TASKS #
	// #############################

	private final Map<REFITSchedulerTaskType, Collection<REFITSchedulerTask>> unassignedTasks = new EnumMap<>(REFITSchedulerTaskType.class);
	public final boolean isSingleThreaded;

	public REFITSchedulerGroup(boolean isSingleThreaded) {
		this.isSingleThreaded = isSingleThreaded;
	}

	public void registerTask(REFITSchedulerTask task) {
		synchronized (unassignedTasks) {
			unassignedTasks.computeIfAbsent(task.taskType, k -> new ArrayList<>()).add(task);
		}
	}

	public Collection<REFITSchedulerTask> takeTasks(REFITSchedulerTaskType taskType) {
		synchronized (unassignedTasks) {
			return unassignedTasks.remove(taskType);
		}
	}

	public void assertAllTaskAreAssigned() {
		if (!checkTaskAssignments()) {
			REFITLogger.logError(this, "Not all tasks have been assigned to a scheduler");
			REFITLogger.logError(this, "Unassigned tasks " + dumpUnassignedTasks());
			throw new IllegalStateException("All tasks must get assigned to a scheduler");
		}
	}

	public boolean checkTaskAssignments() {
		synchronized (unassignedTasks) {
			return unassignedTasks.isEmpty();
		}
	}

	public String dumpUnassignedTasks() {
		synchronized (unassignedTasks) {
			return "" + unassignedTasks;
		}
	}

}
