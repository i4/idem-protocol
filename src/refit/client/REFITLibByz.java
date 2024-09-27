package refit.client;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import refit.agreement.idem.IDEMMessage.IDEMReject;
import refit.agreement.idem.execution.IDEMRejectCertificate;
import refit.client.policy.REFITClientPolicy;
import refit.communication.REFITNetworkEvents.REFITInitialConnectionsCompleted;
import refit.config.REFITConfig;
import refit.crypto.REFITMessageAuthentication;
import refit.message.REFITAckReply;
import refit.message.REFITEvent;
import refit.message.REFITReply;
import refit.message.REFITRequest;
import refit.message.REFITRequest.RequestMode;
import refit.replica.REFITBaseReplica;
import refit.message.REFITRequestWrapper;
import refit.message.REFITUniqueID;
import refit.scheduler.REFITSchedulerGroup;
import refit.util.REFITIntervalStatistics;
import refit.util.REFITLogger;
import refit.util.REFITTime;


public class REFITLibByz extends REFITClientLibrary {

	private short replyReplicaID;
	private final REFITMessageAuthentication messageAuthentication;
	private final REFITReplyCertificate certificate;
	private ResultState resultState;
	private final Semaphore stableReplyLock;
	private final Semaphore stableAfterRejectLock;
	private boolean readWriteConflict;

	private boolean reject;
	private IDEMRejectCertificate rejectCertificate;

	private boolean isConnected;
	private REFITInvocationInstruction delayedInstruction;

	private short primaryID;
	private final int[] executorViews;
	private final boolean[] retryGroup;
	private final short retryGroupSize;

	private long nextSeqNr;
	private REFITRequest request;

	public REFITLibByz(short clientID, REFITSchedulerGroup group, REFITIntervalStatistics statistics, REFITIntervalStatistics rejectStatistics,
	                   REFITClientPolicy policy, boolean isFirstClient, LinkedBlockingQueue<Object> sidechannel) {
		super(clientID, group, statistics, rejectStatistics, policy, isFirstClient, sidechannel);

		short replyOffset = (short) (clientID % (REFITConfig.TOTAL_NR_OF_REPLICAS));
		for (short i = 0; i < REFITConfig.TOTAL_NR_OF_REPLICAS; i++) {
			if (replyOffset > 0) {
				replyOffset--;
			} else {
				this.replyReplicaID = i;
				break;
			}
		}

		this.replyReplicaID = replyOffset;
		this.messageAuthentication = new REFITMessageAuthentication(clientID);
//		REFITLogger.logEvent(this, "Created client at location " + myLocation);

		this.certificate = new REFITReplyCertificate(messageAuthentication, statistics, REFITBaseReplica.ALL_REPLICAS);
		this.stableReplyLock = new Semaphore(0);
		this.stableAfterRejectLock = new Semaphore(0);

		if (REFITConfig.FLOW_CONTROL) {
			this.reject = false;
			int threshold = (REFITConfig.APPLICATION_REPLICA == refit.replica.IDEMReplica.class) ? REFITConfig.FAULTS_TO_TOLERATE + 1 : 1;
			this.rejectCertificate = new IDEMRejectCertificate(threshold);
		} else {
			this.rejectCertificate = null;
		}

		primaryID = REFITBaseReplica.getPrimary(REFITConfig.INITIAL_VIEW);
		executorViews = new int[REFITConfig.TOTAL_NR_OF_REPLICAS];
		// this is usually identical to the normal group of ordering replicas
		retryGroup = REFITBaseReplica.ALL_REPLICAS;
		retryGroupSize = REFITConfig.TOTAL_NR_OF_REPLICAS;
		resultState = ResultState.NONE;

		this.nextSeqNr = 0L;
		this.invocationTimeStamp = -1L;
		this.isConnected = false;
	}

	@Override
	public long getProgress() {
		return nextSeqNr;
	}

	@Override
	public String toString() {
		return String.format("LIBBYZ[%03d]", getNodeID());
	}


	// ######################
	// # CLIENT INTERACTION #
	// ######################

	private long invocationTimeStamp;

	@Override
	public ByteBuffer invoke(ByteBuffer request, long executeAfter, RequestMode mode) throws InterruptedException, REFITRejectException {
		// Insert invocation request
		invokeAsync(request, REFITConfig.ONLY_CONSISTENT ? -1 : executeAfter, REFITConfig.ONLY_CONSISTENT ? mode.toStrongMode() : mode);

		long startMs = REFITTime.currentTimeMillis.getAsLong();
		this.resultAfterReject = false;
		// Wait until reply is stable
		while (true) {
			long remainingWaitMs = REFITConfig.CLIENT_REQUEST_TIMEOUT - (REFITTime.currentTimeMillis.getAsLong() - startMs);
			boolean success;
			success = stableReplyLock.tryAcquire(remainingWaitMs, TimeUnit.MILLISECONDS);
			if (reject && REFITConfig.APPLICATION_REPLICA == refit.replica.IDEMReplica.class) {
				// Wait for a smaller timeout as this request may still be executed.
				remainingWaitMs = REFITConfig.REJECT_TIMEOUT - (REFITTime.currentTimeMillis.getAsLong() - startMs);
				success = stableAfterRejectLock.tryAcquire(remainingWaitMs, TimeUnit.MILLISECONDS);
				if (success && !reject) {
					this.resultAfterReject = true;
				}
			}
			if (success || reject) break;

			if (REFITTime.currentTimeMillis.getAsLong() - startMs < REFITConfig.CLIENT_REQUEST_TIMEOUT) {
				// REFITTime compensates for debugging pauses, however, that is not the case for the tryAcquire method
				// thus compensate manually if necessary
				continue;
			}

			// Retry
			tell(new REFITInvocationInstruction());

			startMs = REFITTime.currentTimeMillis.getAsLong();
		}

		// Handle reject, try again after timeout
		if (this.reject) {
			throw new REFITRejectException(rejectCertificate.fullyRejected());
		}

		// Return result; return null if just acked
		return (resultState.equals(ResultState.STABLE)) ? ((REFITReply) certificate.result).getPayload() : null;
	}

	@Override
	public void invokeAsync(ByteBuffer request, long executeAfter, RequestMode mode) {
		tell(new REFITInvocationInstruction(request, executeAfter, mode));
		invocationTimeStamp = REFITTime.currentTimeMillis.getAsLong();
	}

	@Override
	public boolean isResultStable() {
		// Check whether result is stable
		boolean resultStable = certificate.isStable();
		if (resultStable) return true;

		// Check for retry
		long now = REFITTime.currentTimeMillis.getAsLong();
		if ((now - invocationTimeStamp) > REFITConfig.CLIENT_REQUEST_TIMEOUT) {
			// Retry
			tell(new REFITInvocationInstruction());
		}
		return false;
	}

	@Override
	public ByteBuffer getResult() {
		stableReplyLock.acquireUninterruptibly();
		// Return result; return null if just acked
		return (resultState.equals(ResultState.STABLE)) ? ((REFITReply) certificate.result).getPayload() : null;
	}

	@Override
	public boolean isReadWriteConflict() {
		return readWriteConflict;
	}

	@Override
	public long getAssignedSeqNr() {
		switch (resultState) {
		case NONE:
			return -1;
		case ACK:
		case STABLE:
			return certificate.result.sequenceNumber();
		}
		throw new InternalError("should never happen");
	}

	// #######################
	// # REPLICA INTERACTION #
	// #######################

	@Override
	protected void handleMessage(REFITEvent message) {
		if (message.getClass() == REFITReply.class) {
			handleReply((REFITReply) message);
		} else if (message.getClass() == REFITAckReply.class) {
			handleAck((REFITAckReply) message);
		} else if (message.getClass() == REFITInvocationInstruction.class) {
			handeInvocation((REFITInvocationInstruction) message);
		} else if (message.getClass() == REFITInitialConnectionsCompleted.class) {
			REFITInitialConnectionsCompleted init = (REFITInitialConnectionsCompleted) message;
			if (init.nodeID == getNodeID()) {
				handleInitCompleted();
			} else {
				completeDelayedConnect();
			}
		} else if (message.getClass() == IDEMReject.class){
			handleReject((IDEMReject) message);
		} else {
			REFITLogger.logError(this, "drop message of unexpected type " + message.getClass().getCanonicalName());
		}
	}

	private void handleReply(REFITReply reply) {
		// Ignore replies that arrive after the result has become stable
		if (request == null || (!request.mode.equals(RequestMode.PROBE) && certificate.isStable())) {
			return;
		}

		// Add reply to certificate
		certificate.add(reply);
		if (!certificate.isStable()) {
			if (!certificate.isCompletable()) {
				// immediately trigger retry if the request can't be completed
				tell(new REFITInvocationInstruction());
			}
			return;
		}

		updateReplyReplica(certificate.getReplyTimes());
		updatePrimary(certificate.getPerExecutorViewId());

		// Release lock
		resultState = ResultState.STABLE;
		// Receive stable result after request was already rejected
		if (REFITConfig.FLOW_CONTROL && rejectCertificate.deferred()) {
			stableAfterRejectLock.release();
			reject = false;
		} else {
			stableReplyLock.release();
		}
		if (client != null) client.progress();
	}

	private void handleReject(IDEMReject reject) {
		if (request == null || !reject.uid.equals(request.uid)) return;
		// Ignore rejects after the result or reject certificate has become stable
		if (certificate.isStable()) return;

		if (rejectCertificate.deferred()) {
			rejectCertificate.reject(reject);
			if (rejectCertificate.fullyRejected()) {
				stableAfterRejectLock.release();
			}
		} else {
			rejectCertificate.reject(reject);
			if (rejectCertificate.deferred()) {
				this.reject = true;
				stableReplyLock.release();
			}
		}
	}

	private void updateReplyReplica(short[] replyTimes) {
		short fastestReplicaID = 0;
		short fastestReplyTime = replyTimes[0];
		for (short i = 0; i < replyTimes.length; i++) {
			if (replyTimes[i] < fastestReplyTime) {
				fastestReplicaID = i;
				fastestReplyTime = replyTimes[i];
			}
		}

		replyReplicaID = fastestReplicaID;
	}

	private void updatePrimary(int[] perExecutorViewId) {
		for (int i = 0; i < perExecutorViewId.length; i++) {
			executorViews[i] = Math.max(executorViews[i], perExecutorViewId[i]);
		}

		int[] viewIds = Arrays.copyOf(executorViews, executorViews.length);
		Arrays.sort(viewIds);
		int mergedViewId = REFITConfig.BYZANTINE ? viewIds[viewIds.length / 2] : viewIds[viewIds.length-1];
		short oldPrimary = primaryID;
		primaryID = REFITBaseReplica.getPrimary(mergedViewId);
		replyReplicaID = primaryID;
		if (primaryID != oldPrimary)
			REFITLogger.logWarning(this, String.format("Setting leader to %s%n", primaryID));
	}

	private void handleAck(REFITAckReply ack) {
		// Ignore ACKs once we have enough
		if (certificate.isStable()) return;

		// Add ACK to certificate
		certificate.add(ack);
		if (!certificate.isStable()) return;

		// Release lock
		resultState = ResultState.ACK;
		stableReplyLock.release();
		if (client != null) client.progress();
	}

	private void handleInitCompleted() {
		isConnected = true;
		if (delayedInstruction != null) {
			tell(delayedInstruction);
			delayedInstruction = null;
		}
	}

	private void handeInvocation(REFITInvocationInstruction instruction) {
		// block instructions until we are connected
		if (!isConnected) {
			if (delayedInstruction == null) {
				delayedInstruction = instruction;
			}
			return;
		}

		if (instruction.request != null) {
			resultState = ResultState.NONE;
			// Prepare request
			REFITUniqueID uid = new REFITUniqueID(getNodeID(), nextSeqNr++);
			request = REFITRequest.builder(uid, instruction.request)
					.executeAfter(instruction.executeAfter)
					.replyReplicaID(replyReplicaID)
					.mode(instruction.mode)
					.build();

			certificate.init(uid, request.mode.isTotalOrder(), request.mode.equals(RequestMode.WRITE_ACK));
			if (REFITConfig.FLOW_CONTROL) rejectCertificate.init(uid);
			reject = false;
			readWriteConflict = false;

			if (REFITConfig.CLIENT_MULTICAST ||
					(request.mode.isReadOnly() && (REFITConfig.USE_PBFT_READ_OPTIMIZATION || !request.mode.isTotalOrder()))) {
				multicastRequest(request, REFITBaseReplica.ALL_REPLICAS, REFITConfig.TOTAL_NR_OF_REPLICAS);
			} else {
				unicastRequestToPrimary(request);
			}
		} else if (request != null && resultState.equals(ResultState.NONE)) {
			// ignore retry instruction if the result is already stable
			if (request.mode.equals(RequestMode.PROBE)) {
				throw new InternalError("Can't retry a probe! " + request);
			}

			if (request.mode.toPanicMode() != request.mode) {
				// Send read-only as normal request instead, if we couldn't get a reply in time - doesn't need to be marked panic the first time
				REFITUniqueID uid = new REFITUniqueID(getNodeID(), nextSeqNr++);
				REFITLogger.logWarning(this, "Failed receiving reply to read-only request " + request + ", resending as read-write");
				// Reinitialize certificates
				certificate.init(uid, true, false);
				if (REFITConfig.FLOW_CONTROL) rejectCertificate.init(uid);

				request = REFITRequest.builder(uid, request.getPayload())
						.executeAfter(request.executeAfter)
						.mode(request.mode.toPanicMode())
						.build();
				readWriteConflict = true;

				// send first retry after mode change only to the primary
				unicastRequestToPrimary(request);
			} else {
				// Just resend normal requests
				REFITLogger.logWarning(this, String.format("retry %s (replies from %s)", request, certificate.replies.getVoteList()));
				request = REFITRequest.builder(request.uid, request.getPayload())
						.executeAfter(request.executeAfter)
						.mode(request.mode)
						.build();
				request.markPanic();

				if (!REFITConfig.SIGNED_REQUESTS) {
					multicastRequest(request, retryGroup, retryGroupSize);
				} else {
					multicastRequest(request, REFITBaseReplica.ALL_REPLICAS, REFITConfig.TOTAL_NR_OF_REPLICAS);
				}
			}
		}
		progress();
	}

	private void unicastRequestToPrimary(REFITRequest request) {
		if (!REFITConfig.SIGNED_REQUESTS) {
			// Append MAC
			request.setPaddingSize(REFITMessageAuthentication.calculateMACSize(REFITConfig.TOTAL_NR_OF_REPLICAS));
			request.serializeMessage();
			messageAuthentication.appendMulticastMAC(request, REFITBaseReplica.ALL_REPLICAS);
			// Send request to primary
			sendToReplica(request, primaryID);
		} else {
			request.serializeMessage();
			// use signatures to prevent executors from damaging the request's authenticator
			messageAuthentication.appendSignature(request);

			// wrap
			REFITRequestWrapper wrapper = new REFITRequestWrapper(request.uid, getNodeID(), request);
			wrapper.serializeMessage();
			messageAuthentication.appendUnicastMAC(wrapper, primaryID);

			sendToReplica(wrapper, primaryID);
		}
	}

	private void multicastRequest(REFITRequest request, boolean[] executionReplicas, short recipientCount) {
		if (!REFITConfig.SIGNED_REQUESTS) {
			// Append MAC, special case for executors
			request.setPaddingSize(REFITMessageAuthentication.calculateMACSize(recipientCount));
			request.serializeMessage();
			messageAuthentication.appendMulticastMAC(request, executionReplicas);
			// Send directly to the execution replicas
			sendToReplicas(request, executionReplicas);
		} else {
			request.serializeMessage();
			// use signatures to prevent executors from damaging the request's authenticator
			messageAuthentication.appendSignature(request);

			for (short i = 0; i < executionReplicas.length; i++) {
				if (!executionReplicas[i]) continue;
				// wrap
				REFITRequestWrapper wrapper = new REFITRequestWrapper(request.uid, getNodeID(), request);
				wrapper.serializeMessage();
				messageAuthentication.appendUnicastMAC(wrapper, i);

				sendToReplica(wrapper, i);
			}
		}
	}

}
