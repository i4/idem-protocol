package refit.client;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.concurrent.LinkedBlockingQueue;

import refit.client.policy.REFITClientPolicy;
import refit.communication.REFITNetworkEndpoint;
import refit.communication.REFITNetworkEndpointWorker;
import refit.communication.REFITNetworkInboundRouter;
import refit.communication.REFITNodeAddress;
import refit.config.REFITConfig;
import refit.message.REFITMessage;
import refit.message.REFITMessageType;
import refit.message.REFITRequest.RequestMode;
import refit.scheduler.REFITSchedulerGroup;
import refit.scheduler.REFITSchedulerTaskType;
import refit.stage.REFITStage;
import refit.util.REFITIntervalStatistics;
import refit.util.REFITLogger;

public abstract class REFITClientLibrary extends REFITStage implements REFITNetworkInboundRouter {

	public enum ResultState {
		NONE, ACK, STABLE
	}

	private final REFITSchedulerGroup group;
	private final short clientID;
	protected final REFITNetworkEndpoint endpoint;
	protected final REFITIntervalStatistics statistics;
	protected final REFITIntervalStatistics rejectStatistics;
	protected final REFITClientPolicy policy;
	protected REFITClientTask client;

	protected final LinkedBlockingQueue<Object> sidechannel;

	// Only used for statistics
	public boolean resultAfterReject;

	public REFITClientLibrary(short clientID, REFITSchedulerGroup group, REFITIntervalStatistics statistics,
							  REFITIntervalStatistics rejectStatistics,
	                          REFITClientPolicy policy, boolean isFirstClient, LinkedBlockingQueue<Object> sidechannel) {
		super(REFITSchedulerTaskType.CLIENT_LIBRARY, group);
		this.group = group;
		this.clientID = clientID;
		this.endpoint = createNetwork(isFirstClient);
		if (endpoint != null) {
			endpoint.initCompletedSender.addListener(this);
		}
		this.statistics = statistics;
		this.rejectStatistics = rejectStatistics;
		this.policy = policy;
		this.sidechannel = sidechannel;
	}

	protected REFITNetworkEndpoint createNetwork(boolean isFirstClient) {
		HashSet<Short> initialNodes = new HashSet<>();
		boolean[] connects = getInitialConnects();
		ArrayList<REFITNodeAddress> initialConnects = new ArrayList<>();
		for (short i = 0; i < REFITConfig.TOTAL_NR_OF_REPLICAS; i++) {
			if (connects != null && !connects[i]) continue;
			initialNodes.add(i);
			initialConnects.add(new REFITNodeAddress(i, REFITConfig.ADDRESSES[i]));
		}

		return new REFITNetworkEndpoint(this,
				REFITConfig.TOTAL_NR_OF_REPLICAS,
				(i) -> new REFITNetworkEndpointWorker.ConnectionParameters(REFITSchedulerTaskType.CLIENT_ENDPOINT_WORKER,
						REFITConfig.CLIENT_RECEIVE_BUFFER_SIZE, REFITConfig.CLIENT_SEND_BUFFER_SIZE), REFITConfig.CLIENT_INDIVIDUAL_NETWORK_CONNECTORS,
				initialNodes, false, initialConnects, false, !isFirstClient);
	}

	protected boolean[] getInitialConnects() {
		return null;
	}

	public void setClient(REFITClientTask client) {
		this.client = client;
	}

	@Override
	public REFITSchedulerGroup getGroup() {
		return group;
	}

	@Override
	public short getNodeID() {
		return clientID;
	}

	public REFITIntervalStatistics getStatistics() {
		return statistics;
	}

	public REFITIntervalStatistics getRejectStatistics() {
		return rejectStatistics;
	}
 
	public abstract long getProgress();

	public REFITClientPolicy getPolicy() {
		return policy;
	}

	protected void completeDelayedConnect() {
		// should be called when receiving a REFITInitialConnectionsCompleted instruction for an other node
		endpoint.completeInitialConnects();
	}

	public void addInitialCompleteListener(REFITStage listener) {
		endpoint.initCompletedSender.addListener(listener);
	}

	// ###################
	// # DECODE MESSAGES #
	// ###################

	@Override
	public void receiveMessage(ByteBuffer message, short nodeID) {
		// Create reply
		REFITMessage reply = REFITMessage.createMessage(message);
		if (reply.type != REFITMessageType.REPLY && reply.type != REFITMessageType.ACK_REPLY
				&& reply.type != REFITMessageType.IDEM_REJECT) {
			REFITLogger.logWarning(this, "Dropping message of unexpected type from replica " + nodeID + " " + reply.type);
			return;
		}

		tell(reply);
	}

	protected void sendToReplica(REFITMessage message, short replicaID) {
		endpoint.unicastMessage(message, replicaID);
	}

	protected void sendToReplicas(REFITMessage message, boolean[] replicas) {
		for (short i = 0; i < REFITConfig.TOTAL_NR_OF_REPLICAS; i++) {
			if (replicas[i]) {
				endpoint.unicastMessage(message, i);
			}
		}
	}

	protected void sendToAllReplicas(REFITMessage message) {
		for (short i = 0; i < REFITConfig.TOTAL_NR_OF_REPLICAS; i++) {
			endpoint.unicastMessage(message, i);
		}
	}

	public LinkedBlockingQueue<Object> getSidechannel() {
		return sidechannel;
	}


	// ###################
	// # ABSTRACT METHOD #
	// ###################

	public ByteBuffer invoke(ByteBuffer request, RequestMode mode) throws Exception {
		return invoke(request, -1, mode);
	}

	public abstract ByteBuffer invoke(ByteBuffer request, long executeAfter, RequestMode mode) throws Exception;

	public void invokeAsync(ByteBuffer request, RequestMode mode) {
		invokeAsync(request, -1, mode);
	}

	public abstract void invokeAsync(ByteBuffer request, long executeAfter, RequestMode mode);

	public abstract boolean isResultStable();

	public abstract boolean isReadWriteConflict();

	public abstract ByteBuffer getResult();

	public abstract long getAssignedSeqNr();
}
