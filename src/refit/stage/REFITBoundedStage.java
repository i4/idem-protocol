package refit.stage;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;

import refit.communication.REFITNetworkEvents.REFITConnectionResetNotification;
import refit.communication.REFITNetworkEvents.REFITConnectionWriteableNotification;
import refit.config.REFITConfig;
import refit.message.REFITEvent;
import refit.scheduler.REFITSchedulerGroup;
import refit.scheduler.REFITSchedulerTask;
import refit.scheduler.REFITSchedulerTaskType;
import refit.scheduler.REFITTimer;


public abstract class REFITBoundedStage extends REFITSchedulerTask implements REFITEventReceiver<REFITEvent> {

	protected final REFITBoundedReplicaContext replica;
	protected final Queue<REFITEvent> incomingMessages;
	protected final Set<REFITOutbox> activeOutboxes;
	protected final List<REFITOutbox> registeredOutboxes;
	protected final Map<Short, Set<REFITOutbox>> waitingOutboxes;
	protected boolean isInterleaved;


	public REFITBoundedStage(REFITSchedulerTaskType taskType, REFITBoundedReplicaContext replica) {
		this(taskType, replica, replica.group);
	}

	public REFITBoundedStage(REFITSchedulerTaskType taskType, REFITSchedulerGroup group) {
		this(taskType, null, group);
	}

	private REFITBoundedStage(REFITSchedulerTaskType taskType, REFITBoundedReplicaContext replica, REFITSchedulerGroup group) {
		super(taskType, group);
		this.replica = replica;
		this.incomingMessages = group.isSingleThreaded ? new LinkedList<>() : new ConcurrentLinkedQueue<>();
		this.activeOutboxes = new LinkedHashSet<>();
		this.registeredOutboxes = new ArrayList<>();
		this.waitingOutboxes = new HashMap<>();
		this.isInterleaved = false;
	}

	protected void markInterleaved() {
		isInterleaved = true;
	}


	// ##################
	// # SCHEDULER TASK #
	// ##################

	@Override
	public void execute() {
		long startTime = (isInterleaved) ? System.nanoTime() : 0;
		// Process messages
		REFITEvent message;
		while ((message = incomingMessages.poll()) != null) {
			if (REFITConfig.TRACE_MESSAGES) {
				try {
					handleMessageInternal(message);
				} catch (Throwable t) {
					Exception e = new Exception("Fake exception to capture message source");
					e.setStackTrace(message.getSte());

					t.addSuppressed(e);
					throw t;
				}
			} else {
				handleMessageInternal(message);
			}
			event();
			if (isInterleaved) {
				long currentTime = System.nanoTime();
				// yield when execution took already more than 1ms
				if (currentTime - startTime > 1000 * 1000 && !incomingMessages.isEmpty()) {
					progress();
					break;
				}
			}
		}

		// Complete stage. Messages inserted into the queue by this method will be processed
		// on the next scheduler sub-iteration
		stageComplete();
		processOutboxes();
	}

	protected REFITTimer newTimer(REFITEvent signal) {
		return newTimer(() -> {
			tell(signal);
		});
	}

	protected REFITTimer newTimer() {
		REFITTimeoutSignal signal = new REFITTimeoutSignal();
		return newTimer(() -> {
			tell(signal);
		});
	}

	// #################
	// # STAGE METHODS #
	// #################

	@Override
	public void tell(REFITEvent message) {
		incomingMessages.add(message);
		progress();
	}

	private void handleMessageInternal(REFITEvent message) {
		if (message.getClass() == REFITConnectionWriteableNotification.class) {
			REFITConnectionWriteableNotification writeable = (REFITConnectionWriteableNotification) message;
			Set<REFITOutbox> waiting = waitingOutboxes.remove(writeable.nodeID);
			if (waiting == null) return;
			for (REFITOutbox outbox : waiting) {
				outbox.notifyWriteable(writeable.nodeID);
				enqueueOutbox(outbox);
			}
			return;
		} else if (message.getClass() == REFITConnectionResetNotification.class) {
			REFITConnectionResetNotification reset = (REFITConnectionResetNotification) message;
			for (REFITOutbox outbox : registeredOutboxes) {
				outbox.requeue(reset.nodeID);
			}
			return;
		}
		handleMessage(message);
	}

	protected abstract void handleMessage(REFITEvent message);

	protected void stageComplete() {
		// Override in sub classes if necessary
	}


	// ###################
	// # OUTBOX HANDLING #
	// ###################

	public void registerOutbox(REFITOutbox outbox) {
		registeredOutboxes.add(outbox);
	}

	public void enqueueOutbox(REFITOutbox outbox) {
		activeOutboxes.add(outbox);
	}

	public void addWaitingOutbox(short nodeID, REFITOutbox outbox) {
		Set<REFITOutbox> waiting = waitingOutboxes.computeIfAbsent(nodeID, k -> new HashSet<>());
		waiting.add(outbox);
	}

	protected void processOutboxes() {
		if (activeOutboxes.isEmpty()) return;
		for (REFITOutbox outbox : activeOutboxes) {
			outbox.trySend();
		}
		activeOutboxes.clear();
	}

}
