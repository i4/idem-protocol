package refit.agreement.idem.execution;

import java.util.HashMap;
import java.util.Random;

import refit.agreement.idem.IDEMEvent.IDEMCheckpointEvent;
import refit.agreement.idem.IDEMEvent.IDEMViewEvent;
import refit.agreement.idem.IDEMEvent.IDEMViewInit;
import refit.agreement.idem.IDEMMessage.IDEMCheckpointMessage;
import refit.agreement.idem.IDEMMessage.IDEMCheckpointRequest;
import refit.agreement.idem.IDEMMessage.IDEMCommit;
import refit.agreement.idem.IDEMMessage.IDEMPropose;
import refit.agreement.idem.IDEMMessage.IDEMReject;
import refit.agreement.idem.IDEMMessage.IDEMRequire;
import refit.agreement.idem.IDEMMessage.IDEMForwardRequest;
import refit.agreement.idem.IDEMRequestSet;
import refit.agreement.idem.IDEMStage;
import refit.agreement.idem.order.IDEMWindow;
import refit.application.REFITApplicationServer;
import refit.application.REFITResult;
import refit.config.REFITConfig;
import refit.message.REFITEvent;
import refit.message.REFITReply;
import refit.message.REFITRequest;
import refit.message.REFITUniqueID;
import refit.scheduler.REFITSchedulerTaskType;
import refit.scheduler.REFITTimer;
import refit.stage.REFITObserver;
import refit.stage.REFITReplicaContext;
import refit.util.REFITLogger;

public class IDEMExecutionStage extends IDEMStage {

	private final IDEMRequestSet<REFITRequest> pendingRequests;
	private final HashMap<REFITUniqueID, REFITRequest> cachedRequests;
	private long next;
	private int view;
	private long[] executed;

	// Store the latest rejected request for each client as there is a good chance
	// that this will still be executed. We only want to store the latest request for
	// a client, though. If a replica has multiple requests of a client pending, it
	// is lagging behind anyway and storing a large amount of additional requests will
	// only further slow it down. Better to wait for a checkpoint or a forward in this case.
	// As it is a lagging replica, waiting for a forward/checkpoint will not affect the
	// system's performance as long as there is no fault.
	private final IDEMRecentlyRejected recentlyRejected;
	private final IDEMRejectQueue rejectQueue;

	private IDEMWindow commits;
	private IDEMResult[] results;
	public IDEMCheckpoint checkpoint;
	private IDEMCheckpointMessage checkpointMessage;
	private long checkpointRequested;


	private REFITTimer forwardTimer;
	private REFITTimer vcTimer;

	private boolean awaitingVC;
	private final REFITApplicationServer app;

	public final REFITObserver<IDEMPropose> requireSender;

	public final REFITObserver<IDEMCheckpointEvent> checkpointSender;
	public final REFITObserver<IDEMViewEvent> viewSender;

	public IDEMExecutionStage(REFITReplicaContext replica, REFITApplicationServer application) {
		super(REFITSchedulerTaskType.EXECUTION_STAGE, replica);

		this.pendingRequests = new IDEMRequestSet();
		this.cachedRequests = new HashMap<>(REFITConfig.CHECKPOINT_INTERVAL);
		this.next = 0;
		this.view = 0;
		this.executed = new long[REFITConfig.TOTAL_NR_OF_CLIENTS];

		if (REFITConfig.REJECT_GRACE == 0) {
			this.recentlyRejected = new IDEMRecentlyRejected();
			this.rejectQueue = null;
		} else {
			this.rejectQueue = new IDEMRejectQueue(replica);
			this.recentlyRejected = rejectQueue.recentlyRejected;
		}

		this.commits = new IDEMWindow(0, REFITConfig.MAXIMUM_ORDER_INSTANCES_IN_PROGRESS);
		this.results = new IDEMResult[REFITConfig.TOTAL_NR_OF_CLIENTS];

		this.awaitingVC = false;
		this.app = application;

		// Initial checkpoint
		this.checkpoint = new IDEMCheckpoint(next, executed, results, app.createCheckpoint());
		this.checkpointMessage = null;
		this.checkpointRequested = 0;

		// Senders
		this.requireSender = new REFITObserver<>();
		this.checkpointSender = new REFITObserver<>();
		this.viewSender = new REFITObserver<>();
	}

	@Override
	public void init() {
		super.init();
		this.forwardTimer = newTimer(() -> forwardTimeout());
		this.forwardTimer.start(REFITConfig.FORWARD_TIMEOUT);
		this.vcTimer = newTimer(() -> viewChangeTimeout());
		viewSender.tell(new IDEMViewInit(vcTimer));
	}

	@Override
	protected void handleMessage(REFITEvent message) {
		if (message.getClass() == REFITRequest.class) {
			REFITRequest request = (REFITRequest) message;
			if (request.isForward) {
				handleForward((REFITRequest) message);
			} else {
				handleClientRequest((REFITRequest) message);
			}
		} else if (message.getClass() == IDEMForwardRequest.class) {
			handleForwardRequest((IDEMForwardRequest) message);
		} else if (message.getClass() == IDEMCommit.class) {
			handleCommit((IDEMCommit) message);
		} else if (message.getClass() == IDEMCheckpointRequest.class) {
			handleCheckpointRequest((IDEMCheckpointRequest) message);
		} else if (message.getClass() == IDEMCheckpointMessage.class) {
			handleCheckpoint(((IDEMCheckpointMessage) message).checkpoint);
		} else if (message.getClass() == IDEMViewEvent.class) {
			updateView(((IDEMViewEvent) message).view);
		} else {
			REFITLogger.logError(this, "drop message of unexpected type " + message.getClass().getCanonicalName());
		}

	}

	private void handleClientRequest(REFITRequest r) {
		if (r.uid.seqNr < executed[r.uid.nodeID]) {
			// Resend reply
			if (results[r.uid.nodeID].uid.equals(r.uid)) {
				IDEMResult result = results[r.uid.nodeID];
				REFITReply reply = new REFITReply(r.uid, replica.id, (short) 0, result.view, result.sqn , true, result.result.getReply(), result.mode, replica.id);
				reply.markVerified();
				reply.serializeMessage();
				replica.sendMessageToClient(reply, r.uid.nodeID);
			}
			return;
		}
		if (pendingRequests.containsKey(r.uid)) return;

		// Reject request if we don't have space for a new request right now
		double load = pendingRequests.size() / (double) REFITConfig.REJECT_THRESHOLD;
		if (load >= 1) {
			rejectRequest(r);
			return;
		} else if (REFITConfig.ACTIVE_QUEUE_MANAGEMENT && load >= 0.6) {
			// Active Queue Management: Reject requests pre-emptive if registering a high load
			int activeClientGroup = REFITConfig.currentClientGroup();
			int clientGroup = r.uid.nodeID % REFITConfig.CLIENT_GROUPS;
			if (activeClientGroup != clientGroup) {
				Random randomGenerator = new Random(activeClientGroup + r.uid.nodeID);
				if (randomGenerator.nextDouble() < load) {
					rejectRequest(r);
					return;
				}
			}
		}
		if (REFITLogger.LOG_EXECUTION) REFITLogger.logExecution(this, "Accepting request " + r.uid);
		processRequest(r);
	}

	private void rejectRequest(REFITRequest r) {
		if (REFITLogger.LOG_EXECUTION) REFITLogger.logExecution(this, "Rejecting request " + r.uid + ": " + pendingRequests.size());
		if (REFITConfig.REJECT_GRACE == 0) {
			// Since a request is ordered if one replica accepts it, chances are
			// that this request will be ordered anyway. Hence it might prove useful to store it
			// in a temporary reject-cache to increase throughput/latency.
			// Only store the latest request for each client, as only this is important for
			// up-to-date replicas.
			recentlyRejected.add(r);

			IDEMReject reject = new IDEMReject(r.uid, replica.id);
			reject.serializeMessage();
			reject.markVerified();
			replica.sendMessageToClient(reject, r.uid.nodeID);
		} else {
			rejectQueue.add(r);
		}
	}

	private void handleForward(REFITRequest r) {
		if (r.uid.seqNr < executed[r.uid.nodeID]) return;
		if (pendingRequests.containsKey(r.uid)) {
			// Forward request to proposer
			IDEMRequire require = new IDEMRequire(r.uid, replica.id);
			tellAll(require, requireSender);
			return;
		}
		// Potentially remove request from the reject queue
		if (REFITConfig.REJECT_GRACE != 0) rejectQueue.remove(r.uid);

		// Never reject requests forwarded by other replicas
		if (REFITLogger.LOG_EXECUTION) REFITLogger.logExecution(this, "Accept forwarded request " + r.uid + " from " + r.from);
		processRequest(r);
	}

	private void processRequest(REFITRequest r) {
		r.timestamp = System.currentTimeMillis() + REFITConfig.FORWARD_TIMEOUT;
		pendingRequests.put(r.uid, r);

		// Try executing commands (in case this was already ordered)
		tryExecute();

		if (r.uid.seqNr >= executed[r.uid.nodeID]) {
			IDEMRequire require = new IDEMRequire(r.uid, replica.id);
			if (REFITLogger.LOG_ORDER) REFITLogger.logOrder(this, "Sending Require " + require.uid);
			tellAll(require, requireSender);
		}
	}

	private void handleForwardRequest(IDEMForwardRequest fr) {
		if (REFITLogger.LOG_EXECUTION) REFITLogger.logExecution(this,"Received forward request " + fr.uid);
		REFITRequest r = null;
		if (fr.uid.seqNr < executed[fr.uid.nodeID]) {
			r = cachedRequests.get(fr.uid);
			if (r == null) {
				sendCheckpoint(fr.from);
			}
		} else {
			r = pendingRequests.get(fr.uid);	
		}
		if (r != null) {
			if (REFITLogger.LOG_EXECUTION) REFITLogger.logExecution(this, "Replying to forward request for " + fr.uid + " from " + fr.from);
			if (!r.isForward) {
				r.markForward();
				r.serializeMessage(true); // Force re-serialization of message
			}
			r.markVerified();
			replica.replicaUnicast(r, fr.from);
		}
	}

	private void handleCommit(IDEMCommit c) {
		if (c.view < view) return;
		if (c.view > view) {
			view = c.view;
			viewSender.tell(new IDEMViewEvent(view, false));
		}
		if (!commits.newOpinion(c)) return;

		if (REFITLogger.LOG_ORDER) REFITLogger.logOrder(this, "Received new commit for " + c.uid + " | " + commits.start  + ", " + next);
		commits.add(c);

		// Stop global view change timeout
		awaitingVC = false;
		vcTimer.clear();

		// Request checkpoint if window is too far ahead
		if (commits.start > next && commits.start > checkpointRequested) {
			if (REFITLogger.LOG_CHECKPOINT) REFITLogger.logCheckpoint(this, "Request checkpoint for " + commits.start);
			IDEMCheckpointRequest ckptReq = new IDEMCheckpointRequest(commits.start, replica.id);
			// Avoid requesting the same checkpoint multiple times
			this.checkpointRequested = commits.start + REFITConfig.CHECKPOINT_INTERVAL;
			tellOthers(ckptReq);
		} else {
			tryExecute();
		}
	}

	private boolean requestedForward = false;
	private void tryExecute() {
		// Still waiting for a checkpoint
		if (next < commits.start) return;

		for (long n = next; n <= commits.end; n++) {
			if (!commits.match(n, REFITConfig.FAULTS_TO_TOLERATE + 1, replica.id)) break;

			// Get request
			REFITUniqueID id = commits.get(n).id;

			// Duplicate detection: only execute request if not already
			if (id.seqNr >= executed[id.nodeID]) {
				REFITRequest request = pendingRequests.get(id);
				// We don't have the corresponding request yet
				if (request == null) {
					// Check if we recently rejected this request
					request = recentlyRejected.get(id);
					if (request == null) {
						// Request forward for this request
						if (!requestedForward) {
							if (REFITLogger.LOG_EXECUTION) REFITLogger.logExecution(this, "Requesting forward for " + id);
							IDEMForwardRequest forwardRequest = new IDEMForwardRequest(id, replica.id);
							tellOthers(forwardRequest);
							requestedForward = true;
						}
						break;
					}
				}
				requestedForward = false;

				// Execute request and update state
				if (REFITLogger.LOG_EXECUTION) REFITLogger.logExecution(this, "Executing on " + n + ": " + request.uid);
				REFITResult result = app.processRequest(request.uid, n, request.getPayload());
				REFITReply reply = new REFITReply(request.uid, replica.id, (short) 0, view, n, true, result.getReply(), request.mode, replica.id);
				reply.markVerified();
				reply.serializeMessage();
				replica.sendMessageToClient(reply, reply.uid.nodeID);
				executed[id.nodeID] = id.seqNr + 1;
				results[id.nodeID] = new IDEMResult(id, result, n, view, request.mode);
				pendingRequests.remove(id);
				cachedRequests.put(id, request);
			}

			next = n + 1;
			// Create checkpoint
			if (next % REFITConfig.CHECKPOINT_INTERVAL == 0) {
				if (REFITLogger.LOG_CHECKPOINT) REFITLogger.logCheckpoint(this, "Creating checkpoint for sqn " + next);
				checkpoint = new IDEMCheckpoint(next, executed, results, app.createCheckpoint());
				checkpointMessage = null;
				checkpointSender.tell(new IDEMCheckpointEvent(checkpoint));
				// Delete cached requests now included in checkpoint
				cachedRequests.clear();
			}
		}

		// Fill empty request slots from queue if we have enough space
		if (REFITConfig.REJECT_GRACE != 0) {
			while (pendingRequests.size() < REFITConfig.REJECT_THRESHOLD) {
				// Check whether we have requests waiting to be executed in the rejectQueue
				REFITRequest newRequest = rejectQueue.get();
				if (REFITLogger.LOG_EXECUTION) REFITLogger.logExecution(this, "Checking for new requests in requestQueue: " + (newRequest == null ? "null" : newRequest.uid));
				while (newRequest != null) {
					// Check whether we already processed this request
					if (newRequest.uid.seqNr < executed[newRequest.uid.nodeID] || pendingRequests.containsKey(newRequest.uid)) {
						newRequest = rejectQueue.get();
						continue;
					}
					processRequest(newRequest);
					break;
				}
				if (newRequest == null) break;
			}
		}
	}

	private void handleCheckpointRequest(IDEMCheckpointRequest ckptReq) {
		if (ckptReq.uid.seqNr > checkpoint.sqn) return;
		sendCheckpoint(ckptReq.from);
	}

	private void handleCheckpoint(IDEMCheckpoint ckpt) {
		if (ckpt.sqn <= next) return;

		if (REFITLogger.LOG_CHECKPOINT) REFITLogger.logCheckpoint(this, "Applying new checkpoint for " + ckpt.sqn);
		this.next = ckpt.sqn;
		this.executed = ckpt.executed;
		this.results = ckpt.results;
		this.checkpoint = ckpt;
		this.checkpointMessage = null;
		this.app.applyCheckpoint(ckpt.state);

		checkpointSender.tell(new IDEMCheckpointEvent(checkpoint));

		// Update request stores
		cachedRequests.clear();
		pendingRequests.removeExecuted(executed);

		// Update requests if set
		if (ckpt.requests != null) {
			for (REFITRequest r : ckpt.requests.values()) {
				r.timestamp = System.currentTimeMillis() + REFITConfig.FORWARD_TIMEOUT;
			}
			pendingRequests.putAll(ckpt.requests);
		}

		// Stop global view change timeout
		awaitingVC = false;
		vcTimer.clear();

		requestedForward = false;

		tryExecute();
	}

	private void sendCheckpoint(short to) {
		if (checkpointMessage == null) {
			checkpointMessage = new IDEMCheckpointMessage(checkpoint, replica.id);
			checkpointMessage.serializeMessage();
			checkpointMessage.markVerified();
		}
		if (REFITLogger.LOG_CHECKPOINT) REFITLogger.logCheckpoint(this, "Sending checkpoint " + checkpoint.sqn + " to " + to);
		replica.replicaUnicast(checkpointMessage, to);
	}

	private void updateView(int view) {
		if (view < this.view) return;
		this.view = view;
	}

	private void forwardTimeout() {
		long now = System.currentTimeMillis();
		// Forwarding mechanism
		for (REFITRequest r : pendingRequests.values()) {
			if (r.timestamp + REFITConfig.FORWARD_TIMEOUT <= now) {
				if (!r.isForward) {
					r.markForward();
					r.serializeMessage(true); // Force re-serialization of message
				}
				tellOthers(r);
				// Start view change timeout
				if (!awaitingVC) vcTimer.startIfNotRunning(REFITConfig.REPLICA_REQUEST_TIMEOUT);
			}
		}
		// Restart forwarding timer
		forwardTimer.startIfNotRunning(REFITConfig.FORWARD_TIMEOUT);
	}

	private void viewChangeTimeout() {
		if (REFITLogger.LOG_EXECUTION) REFITLogger.logExecution(this, "Triggering view change: " + view);
		awaitingVC = true;
		view = view + 1;
		viewSender.tell(new IDEMViewEvent(view, true));
	}
}
