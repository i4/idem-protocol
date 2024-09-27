package refit.scheduler;

import javax.management.InstanceNotFoundException;
import javax.management.openmbean.CompositeData;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.util.LinkedList;

import com.sun.management.GarbageCollectionNotificationInfo;
import com.sun.management.GcInfo;
import refit.util.REFITLogger;
import refit.util.REFITTime;

public class REFITSlowStageTracker {

	private final LinkedList<SlowTask> slowTasks = new LinkedList<>();
	private volatile long lastGCDuration;
	private volatile long lastGCEndTime;

	private long lastStart;

	public REFITSlowStageTracker() {
		subscribeToGCEvents();
	}

	public void taskStart() {
		lastStart = System.nanoTime();
	}

	public void taskEnd(REFITSchedulerTask task) {
		long tss = REFITTime.timeSinceStart();
		long taskEndTime = System.nanoTime();
		if (taskEndTime - lastStart > 5 * 1000 * 1000) {
			slowTasks.add(new SlowTask(taskEndTime - lastStart,
					tss, "Slow task " + task));
		}
		lastStart = taskEndTime;
	}

	public void scheduleEnd() {
		while (!slowTasks.isEmpty()) {
			SlowTask head = slowTasks.peekFirst();
			if (head.endTimeInMs - head.durationInNs / 1000 / 1000 - 1 < lastGCEndTime) {
				// Discount GC time from slow stage runs
				long compensatedDurationInNs = head.durationInNs - lastGCDuration * 1000 * 1000;
				if (head.endTimeInMs + 1 < lastGCEndTime) {
					compensatedDurationInNs = head.durationInNs;
				}
				if (compensatedDurationInNs > 5 * 1000 * 1000) {
					REFITLogger.logWarning(this, head.msg + " took " + compensatedDurationInNs / 1000 + " us");
				}
				slowTasks.removeFirst();
			} else {
				break;
			}
		}
	}

	private void subscribeToGCEvents() {
		for (GarbageCollectorMXBean bean : ManagementFactory.getGarbageCollectorMXBeans()) {
			try {
				ManagementFactory.getPlatformMBeanServer().addNotificationListener(bean.getObjectName(), (notification, handback) -> {
					if (notification.getType().equals(GarbageCollectionNotificationInfo.GARBAGE_COLLECTION_NOTIFICATION)) {
						CompositeData cd = (CompositeData) notification.getUserData();
						GarbageCollectionNotificationInfo gcNotificationInfo = GarbageCollectionNotificationInfo.from(cd);
						GcInfo gcInfo = gcNotificationInfo.getGcInfo();
						lastGCDuration = gcInfo.getDuration();
						lastGCEndTime = gcInfo.getEndTime();
//						REFITLogger.logWarning(this, notification.getType() + " end " + lastGCEndTime);
					}
				}, null, null);
			} catch (InstanceNotFoundException e) {
				throw new InternalError(e);
			}
		}
	}


	private static class SlowTask {
		public final long durationInNs;
		public final long endTimeInMs;
		public final String msg;

		public SlowTask(long durationInNs, long endTimeInMs, String msg) {
			this.durationInNs = durationInNs;
			this.endTimeInMs = endTimeInMs;
			this.msg = msg;
		}
	}
}
