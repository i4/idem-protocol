package refit.scheduler;

import java.io.IOException;
import java.nio.channels.SelectableChannel;
import java.nio.channels.SelectionKey;

import refit.util.REFITAssert;


public abstract class REFITSchedulerTask {

	public final REFITSchedulerTaskType taskType;
	private short taskID;

	private REFITSchedulerFacade scheduler;


	public REFITSchedulerTask(REFITSchedulerTaskType taskType, REFITSchedulerGroup group) {
		REFITAssert.assertTrue(taskType != null, "Missing task type");
		this.taskType = taskType;
		this.taskID = -1;
		this.throughput = 0L;
		group.registerTask(this);
	}


	// ################
	// # TASK METHODS #
	// ################

	public void setTaskID(short id) {
		if (this.taskID != -1) {
			throw new IllegalStateException("May set id only once");
		}
		this.taskID = id;
	}

	public final short taskID() {
		return this.taskID;
	}

	public void setScheduler(REFITSchedulerFacade scheduler) {
		this.scheduler = scheduler;
	}

	public SelectionKey registerIO(SelectableChannel channel, int operations) throws IOException {
		return scheduler.registerIOTask(this, channel, operations);
	}

	public void deregisterIO(SelectableChannel channel) {
		scheduler.deregisterIOTask(channel);
	}

	public boolean sameScheduler(REFITSchedulerTask other) {
		return scheduler == other.scheduler;
	}

	public void selectKey(SelectionKey key) {
		throw new IllegalStateException("This scheduler task does not own a SelectionKey " + this);
	}

	protected REFITTimer newTimer(REFITTimer.TimerCallback callback) {
		return scheduler.newTimer(callback);
	}

	protected void progress() {
		if (scheduler != null) scheduler.notifyProgress(this);
	}

	public void init() {
		// Override in sub classes if necessary
	}

	public abstract void execute();


	// ##############
	// # STATISTICS #
	// ##############

	private long throughput;


	protected void event() {
		throughput++;
	}

	public long collectThroughput() {
		long tmp = throughput;
		throughput = 0L;
		return tmp;
	}

}
