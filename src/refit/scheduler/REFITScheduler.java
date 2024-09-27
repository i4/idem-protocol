package refit.scheduler;

import java.io.IOException;
import java.nio.channels.SelectableChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicIntegerArray;

import refit.config.REFITConfig;
import refit.util.REFITTime;


public class REFITScheduler extends Thread implements REFITSchedulerFacade {

	// ####################
	// # SCHEDULER OBJECT #
	// ####################

	private static final int TASK_TYPE_SUB_ITERATION_LIMIT = 64;
	private static final int HANG_TIMEOUT_US = 500 * 1000;

	private final Selector selector;

	private final List<REFITSchedulerTask> assignedTasks;

	private AtomicIntegerArray remoteReadyTasks;
	private ConcurrentLinkedDeque<REFITSchedulerTask> remoteTaskQueue;
	private boolean[] readyTasks;
	private ArrayDeque<REFITSchedulerTask> taskQueue;

	private REFITSchedulerTask[] tasks;
	private boolean[] isQueued;
	private EnumMap<REFITSchedulerTaskType, ArrayDeque<SelectionKey>> queues;
	private int queueSize;

	private final CountDownLatch startupSync;
	private final REFITHangDetector hangDetector;


	public REFITScheduler(CountDownLatch sync, REFITHangDetector detector) {
		// Create list of assigned tasks
		this.assignedTasks = new ArrayList<>();
		this.timeouts = new TreeSet<>();
		startupSync = sync;
		hangDetector = detector;

		// Create selector
		Selector s = null;
		try {
			s = Selector.open();
		} catch (IOException ioe) {
			ioe.printStackTrace();
		}
		this.selector = s;
	}


	// ###################
	// # TASK ASSIGNMENT #
	// ###################

	public void assignTasks(REFITSchedulerGroup group, REFITSchedulerTaskType taskType) {
		Collection<REFITSchedulerTask> entry = group.takeTasks(taskType);
		if (entry == null) return;
		for (REFITSchedulerTask task : entry) assignTask(task);
	}

	private void assignTask(REFITSchedulerTask task) {
		assignedTasks.add(task);
		task.setScheduler(this);
	}

	public SelectionKey registerIOTask(REFITSchedulerTask task, SelectableChannel channel, int operations) throws IOException {
		if (REFITConfig.ENABLE_DEBUG_CHECKS && !assignedTasks.contains(task)) {
			throw new IllegalArgumentException("Task not owned by this scheduler wants to register as io task " + task.toString());
		}
		return channel.register(selector, operations, task);
	}

	public void deregisterIOTask(SelectableChannel channel) {
		SelectionKey key = channel.keyFor(selector);
		if (key == null) {
			throw new IllegalArgumentException("Channel is not registered with this selector!");
		}
		key.cancel();
	}


	// ####################
	// # TIMER MANAGEMENT #
	// ####################

	private final TreeSet<REFITTimer> timeouts;
	private long timerCtr; // used to provide unique ids to timers

	public REFITTimer newTimer(REFITTimer.TimerCallback callback) {
		if (this != Thread.currentThread()) {
			throw new IllegalArgumentException("Timer must be created by the task itself!");
		}
		return new REFITTimer(this, timerCtr++, callback);
	}

	public void queueTimer(REFITTimer timer) {
		if (this != Thread.currentThread()) {
			throw new IllegalArgumentException("Timeout must only be set by the task itself!");
		}
		if (timer.hasExpired()) {
			throw new IllegalStateException("Can't queue an expired timer");
		}
		boolean added = timeouts.add(timer);
		if (!added) {
			throw new IllegalStateException("The timer is already active!");
		}
	}

	public void stopTimer(REFITTimer timer) {
		if (this != Thread.currentThread()) {
			throw new IllegalArgumentException("Timeout must only be set by the task itself!");
		}
		boolean removed = timeouts.remove(timer);
		if (!removed) {
			throw new IllegalStateException("The timer is not active!");
		}
	}


	// ##################
	// # TASK EXECUTION #
	// ##################

	public void notifyProgress(REFITSchedulerTask task) {
		if (this == Thread.currentThread()) {
			if (!readyTasks[task.taskID()]) {
				taskQueue.add(task);
			}
			readyTasks[task.taskID()] = true;
			return;
		}
		if (remoteReadyTasks.getAndSet(task.taskID(), 1) == 0) {
			remoteTaskQueue.add(task);
		}
		selector.wakeup();
	}

	@Override
	public void run() {
		initRun();

		if (startupSync != null) {
			startupSync.countDown();
			try {
				startupSync.await();
			} catch (InterruptedException e) {
				e.printStackTrace();
				System.exit(1);
			}
		}

		// Allow for initialization on the correct thread
		for (REFITSchedulerTask task : tasks) task.init();

		runTasks();
	}

	private void initRun() {
		// Prepare tasks
		tasks = assignedTasks.toArray(new REFITSchedulerTask[0]);
		for (short i = 0; i < tasks.length; i++) tasks[i].setTaskID(i);

		remoteReadyTasks = new AtomicIntegerArray(tasks.length);
		remoteTaskQueue = new ConcurrentLinkedDeque<>();
		readyTasks = new boolean[tasks.length];
		taskQueue = new ArrayDeque<>();

		// store task reference instead of taskID to avoid autoboxing of int to Integer
		queues = new EnumMap<>(REFITSchedulerTaskType.class);
		for (REFITSchedulerTaskType type : REFITSchedulerTaskType.values()) {
			queues.put(type, new ArrayDeque<>());
		}
		isQueued = new boolean[tasks.length];
	}

	private void runTasks() {
		// For a single stage it's usually ok to not return to the scheduler
		REFITHangDetector.Entry watchdog = (hangDetector != null && tasks.length > 1)
				? hangDetector.register(getName(), HANG_TIMEOUT_US) : null;
		REFITSchedulerStatistics stats = (REFITConfig.COLLECT_STAGE_STATISTICS) ? new REFITSchedulerStatistics(tasks) : null;
		REFITSlowStageTracker slow = (REFITConfig.TRACK_SLOW_STAGES) ? new REFITSlowStageTracker() : null;

		selector.wakeup();

		while (!isInterrupted()) {
			// Collect tasks that are able to perform I/O operations
			if (stats != null) stats.selectStart();
			if (watchdog != null) watchdog.disable();

			selectReadyTasks();

			if (watchdog != null) watchdog.refresh();
			if (stats != null) stats.selectEnd();

			// Handle runnable tasks; Called "sub-iteration", process internal messages exhaustively
			// before reading new messages from the network
			if (stats != null) stats.scheduleStart();
			while (!isInterrupted()) {
				// Execute runnable tasks
				boolean progress = false;

				// transfer tasks enqueued by other threads
				while (!remoteTaskQueue.isEmpty()) {
					REFITSchedulerTask task = remoteTaskQueue.pollFirst();
					remoteReadyTasks.set(task.taskID(), 0);
					if (!readyTasks[task.taskID()]) {
						readyTasks[task.taskID()] = true;
						taskQueue.add(task);
					}
				}

				// execute everything
				while (!taskQueue.isEmpty()) {
					REFITSchedulerTask task = taskQueue.pollFirst();
					readyTasks[task.taskID()] = false;
					progress = true;

					if (slow != null) slow.taskStart();
					if (stats != null) stats.taskStart();

					task.execute();

					if (stats != null) stats.taskEnd(task.taskID());
					if (slow != null) slow.taskEnd(task);
				}

				// Break if none of the tasks has been executed in this loop
				if (!progress) break;
			}

			if (stats != null) stats.scheduleEnd();
			if (slow != null) slow.scheduleEnd();
		}
		if (watchdog != null) watchdog.disable();
	}

	private void collectTasks(SelectionKey key) {
		// Collect tasks that are able to perform I/O operations
		REFITSchedulerTask task = (REFITSchedulerTask) key.attachment();
		if (REFITConfig.ENABLE_DEBUG_CHECKS && (task.taskID() >= tasks.length || tasks[task.taskID()] != task)) {
			throw new IllegalArgumentException("Got task from different scheduler " + task);
		}
		if (key.readyOps() != SelectionKey.OP_READ) {
			// always process non-read operations immediately to avoid blocking our output
			task.selectKey(key);
			notifyProgress(task);
		} else if (!isQueued[task.taskID()]) {
			queues.get(task.taskType).add(key);
			isQueued[task.taskID()] = true;
			queueSize++;
		}
	}

	private void selectReadyTasks() {
		// timeout of zero blocks indefinitely
		long timeout = 0;
		if (!timeouts.isEmpty()) {
			timeout = Math.max(0, timeouts.first().currentTimeout() - REFITTime.currentTimeMillis.getAsLong());
		}
		// Block until at least one task becomes runnable
		try {
			if (!timeouts.isEmpty() && timeout == 0 || queueSize > 0) {
				selector.selectNow(this::collectTasks);
			} else {
				selector.select(this::collectTasks, timeout);
			}
		} catch (IOException ioe) {
			ioe.printStackTrace();
			return;
		}

		// Pick only a limited number of tasks per taskType
		for (ArrayDeque<SelectionKey> queue : queues.values()) {
			for (int i = 0; i < TASK_TYPE_SUB_ITERATION_LIMIT && !queue.isEmpty(); i++) {
				SelectionKey key = queue.removeFirst();
				// the queue might have stored keys that were canceled in the meantime
				if (!key.isValid()) {
					i--;
					continue;
				}

				REFITSchedulerTask task = (REFITSchedulerTask) key.attachment();
				task.selectKey(key);
				notifyProgress(task);
				isQueued[task.taskID()] = false;
				queueSize--;
			}
		}

		// Process all timeouts before starting the regular readyTasks iterations
		// This prevents timeouts from keeping a stage permanently active
		long timeMillis = REFITTime.currentTimeMillis.getAsLong();
		while (!timeouts.isEmpty() && timeouts.first().currentTimeout() <= timeMillis) {
			REFITTimer timedOutTimer = timeouts.pollFirst();
			if (timedOutTimer == null) throw new InternalError("Whoopsie");
			timedOutTimer.timeout();
		}
	}
}
