package refit.scheduler;

import refit.util.REFITLogger;
import refit.util.REFITTime;

public class REFITSchedulerStatistics {

	private static final int STATISTICS_INTERVAL = 10000;

	private final REFITSchedulerTask[] tasks;

	private int epochCounter = 0;
	private long epochStartTimestamp = REFITTime.currentTimeMillis.getAsLong();
	private long selectDurations = 0L;
	private long selectDurationsMax = 0L;
	private int[] executions;
	private long[] executionDurations;
	private long[] executionDurationsMax;
	private long[] throughputs;
	private final long[] totalThroughputs;
	private long scheduleLoopMax = 0L;

	// temporary variable
	private long selectDuration;
	private long scheduleLoopDuration;
	private long taskDuration;

	public REFITSchedulerStatistics(REFITSchedulerTask[] tasks) {
		this.tasks = tasks;

		executions = new int[tasks.length];
		executionDurations = new long[tasks.length];
		executionDurationsMax = new long[tasks.length];
		throughputs = new long[tasks.length];
		totalThroughputs = new long[tasks.length];
	}

	public String toString() {
		return "STATS";
	}

	public void selectStart() {
		selectDuration = -REFITTime.nanoTime.getAsLong();
	}

	public void selectEnd() {
		selectDuration += REFITTime.nanoTime.getAsLong();
		selectDurations += selectDuration;
		selectDurationsMax = Math.max(selectDurationsMax, selectDuration);
	}

	public void scheduleStart() {
		epochCounter++;
		scheduleLoopDuration = -REFITTime.nanoTime.getAsLong();
	}

	public void taskStart() {
		taskDuration = -REFITTime.nanoTime.getAsLong();
	}

	public void taskEnd(int taskID) {
		taskDuration += REFITTime.nanoTime.getAsLong();
		executions[taskID]++;
		executionDurations[taskID] += taskDuration;
		executionDurationsMax[taskID] = Math.max(executionDurationsMax[taskID], taskDuration);
		long throughput = tasks[taskID].collectThroughput();
		throughputs[taskID] += throughput;
		totalThroughputs[taskID] += throughput;
	}

	public void scheduleEnd() {
		scheduleLoopDuration += REFITTime.nanoTime.getAsLong();
		scheduleLoopMax = Math.max(scheduleLoopMax, scheduleLoopDuration);

		// Handle statistics
		if (REFITTime.currentTimeMillis.getAsLong() < (epochStartTimestamp + STATISTICS_INTERVAL)) return;
		StringBuilder sb = new StringBuilder(45 + 38 + 82 * tasks.length);
		sb.append(String.format("[STAGE] Epoch %04d: epoll %6.0f (%10d) %7.4f%%\n", epochCounter, selectDurations / (float) epochCounter, selectDurationsMax, selectDurations * 1e-6 / STATISTICS_INTERVAL * 100));
		sb.append(String.format("[STAGE] Iteration max: (%10d)\n", scheduleLoopMax));
		sb.append(String.format("[STAGE]  %-10s | %6s | %8s | %7s (%10s) | %12s %12s | \n", "task", "execs", "perc", "avg dur", "max", "throughput", "total tput"));
		for (int i = 0; i < tasks.length; i++) {
			if (executions[i] == 0) continue;
			sb.append(String.format(
					"[STAGE]  %-10s | %6d | %7.4f%% | %7.0f (%10d) | %12d %12d | \n",
					tasks[i],
					executions[i],
					executionDurations[i] * 1e-6 / STATISTICS_INTERVAL * 100,
					executionDurations[i] / (float) executions[i],
					executionDurationsMax[i],
					throughputs[i],
					totalThroughputs[i]));
		}
		REFITLogger.logDebug(this, sb.toString());

		// Reset statistics
		epochCounter = 0;
		epochStartTimestamp = REFITTime.currentTimeMillis.getAsLong();
		selectDurations = 0L;
		selectDurationsMax = 0L;
		executions = new int[tasks.length];
		executionDurations = new long[tasks.length];
		executionDurationsMax = new long[tasks.length];
		throughputs = new long[tasks.length];
		scheduleLoopMax = 0L;
	}
}
