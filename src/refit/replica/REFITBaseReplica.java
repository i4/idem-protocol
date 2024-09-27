package refit.replica;

import java.lang.reflect.InvocationTargetException;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import refit.application.REFITApplicationServer;
import refit.application.REFITSystemApplicationServer;
import refit.communication.REFITNetworkEndpoint;
import refit.communication.REFITNetworkEndpointWorker;
import refit.communication.REFITNetworkInboundRouter;
import refit.communication.REFITNodeAddress;
import refit.communication.REFITScheduledRouter;
import refit.config.REFITConfig;
import refit.crypto.REFITMessageAuthentication;
import refit.message.REFITMessage;
import refit.message.REFITMessageType;
import refit.message.REFITReplyBase;
import refit.scheduler.REFITHangDetector;
import refit.scheduler.REFITScheduler;
import refit.scheduler.REFITSchedulerGroup;
import refit.scheduler.REFITSchedulerTaskType;
import refit.stage.REFITBoundedStage;
import refit.stage.REFITObserver;
import refit.stage.REFITOutbox;
import refit.stage.REFITReplicaContext;
import refit.util.REFITLogger;


public abstract class REFITBaseReplica implements REFITScheduledRouter, REFITNetworkInboundRouter {
	protected final short id;
	protected final REFITMessageAuthentication mac;
	protected final REFITSchedulerGroup group;

	protected final REFITReplicaContext context;

	private final REFITNetworkEndpoint network;
	private REFITScheduler[] schedulers;
	private final Map<Class<? extends REFITMessage>, REFITObserver<REFITMessage>> inbounds;
	private final REFITObserver<REFITMessage> clientsInbound;

	public static final boolean[] ALL_REPLICAS = new boolean[REFITConfig.TOTAL_NR_OF_REPLICAS];

	static {
		Arrays.fill(ALL_REPLICAS, true);
	}

	public static boolean isPrimary(int view, short id) {
		return view % REFITConfig.TOTAL_NR_OF_REPLICAS == id;
	}

	public static short getPrimary(int view) {
		return (short) (view % REFITConfig.TOTAL_NR_OF_REPLICAS);
	}

	public REFITBaseReplica(short id, boolean isSingleThreaded, boolean createClientNetwork) {
		// Configure replica
		this.id = id;
		this.mac = new REFITMessageAuthentication(id);
		this.group = new REFITSchedulerGroup(isSingleThreaded);
		this.context = createContext();

		this.network = setupNetwork(createClientNetwork);

		this.inbounds = new HashMap<>();
		this.clientsInbound = new REFITObserver<>();
	}

	private REFITReplicaContext createContext() {
		return new REFITReplicaContext(id, mac, group) {
			@Override
			public void replicaMulticast(REFITMessage message, boolean[] replicas) {
				if (REFITLogger.LOG_COMMUNICATION)
					REFITLogger.logCommunication(this, "send message " + message + " to " + Arrays.toString(replicas));

				for (short i = 0; i < replicas.length; i++) {
					if (i == id) continue;
					if (replicas[i]) network.unicastMessage(message, i);
				}
			}

			@Override
			public void replicaUnicast(REFITMessage message, short replicaID) {
				if (REFITLogger.LOG_COMMUNICATION)
					REFITLogger.logCommunication(this, "send message " + message + " to " + replicaID);

				network.unicastMessage(message, replicaID);
			}

			@Override
			public boolean replicaBoundedUnicast(REFITMessage message, short replicaID, REFITOutbox outbox) {
				boolean queued = network.unicastMessageBounded(message, replicaID, outbox.getStage());
				if (!queued) {
					outbox.getStage().addWaitingOutbox(replicaID, outbox);
				}
				return queued;
			}

			@Override
			public void sendMessageToClient(REFITReplyBase reply, short clientID) {
				if (REFITConfig.ENABLE_DEBUG_CHECKS && clientID < REFITConfig.TOTAL_NR_OF_REPLICAS) {
					throw new AssertionError("Got client reply " + reply + " for id " + clientID + " which is not a clientID!");
				}
				// Complains if the client is not connected
				network.unicastMessage(reply, clientID);
			}
		};
	}

	private REFITNetworkEndpoint setupNetwork(boolean createClientNetwork) {
		HashSet<Short> initialNodes = new HashSet<>();
		for (short i = 0; i < REFITConfig.TOTAL_NR_OF_REPLICAS; i++) {
			if (i != id) initialNodes.add(i);
		}
		ArrayList<REFITNodeAddress> initialConnects = new ArrayList<>();
		for (short i = (short) (id + 1); i < REFITConfig.TOTAL_NR_OF_REPLICAS; i++) {
			final SocketAddress addr = (REFITConfig.LOCATION[i] == REFITConfig.LOCATION[id]) ? REFITConfig.INT_ADDRESSES[i] : REFITConfig.ADDRESSES[i];
			initialConnects.add(new REFITNodeAddress(i, addr));
		}

		// Create client and replica networks
		return new REFITNetworkEndpoint(this,
				REFITConfig.TOTAL_NR_OF_REPLICAS + REFITConfig.TOTAL_NR_OF_CLIENTS,
				(i) -> {
					if (i < REFITConfig.TOTAL_NR_OF_REPLICAS) {
						return new REFITNetworkEndpointWorker.ConnectionParameters(REFITSchedulerTaskType.REPLICA_NETWORK_ENDPOINT_WORKER,
								REFITConfig.REPLICA_NETWORK_RECEIVE_BUFFER_SIZE, REFITConfig.REPLICA_NETWORK_SEND_BUFFER_SIZE);
					} else {
						return new REFITNetworkEndpointWorker.ConnectionParameters(REFITSchedulerTaskType.CLIENT_NETWORK_ENDPOINT_WORKER,
								REFITConfig.CLIENT_NETWORK_RECEIVE_BUFFER_SIZE, 0);
					}
				}, REFITConfig.REPLICA_NETWORK_CONNECTORS,
				initialNodes, !createClientNetwork, initialConnects, true, false);
	}

	@Override
	public REFITSchedulerGroup getGroup() {
		return group;
	}

	@Override
	public short getNodeID() {
		return id;
	}

	@Override
	public String toString() {
		return "RPLCA";
	}


	// #################
	// # SETUP HELPERS #
	// #################

	protected abstract void assignTasks(REFITScheduler[] schedulers);

	protected REFITApplicationServer createApplication() {
		try {
			return REFITConfig.APPLICATION_SERVER.getConstructor().newInstance();
		} catch (NoSuchMethodException | IllegalAccessException | InstantiationException | InvocationTargetException e) {
			e.printStackTrace();
			System.exit(1);
			// Strange java things...
			return null;
		}
	}

	protected REFITSystemApplicationServer createSystemApplication() {
		try {
			return REFITConfig.SYSTEM_APPLICATION_SERVER.getConstructor().newInstance();
		} catch (NoSuchMethodException | IllegalAccessException | InstantiationException | InvocationTargetException e) {
			e.printStackTrace();
			System.exit(1);
			// Strange java things...
			return null;
		}
	}

	// ###########################
	// # CLIENT NETWORK ENDPOINT #
	// ###########################

	protected void receiveMessageFromClient(REFITMessage message) {
		clientsInbound.tell(message);
	}

	protected void addToClientInbound(REFITBoundedStage stage) {
		clientsInbound.addListener(stage);
	}


	// ############################
	// # REPLICA NETWORK ENDPOINT #
	// ############################

	protected void addToInitialConnectObserver(REFITBoundedStage stage) {
		network.initCompletedSender.addListener(stage);
	}

	protected void addToInbound(Class<? extends REFITMessage> type, REFITBoundedStage stage) {
		if (!inbounds.containsKey(type)) {
			inbounds.put(type, new REFITObserver<>());
		}
		inbounds.get(type).addListener(stage);
	}

	protected void addClientInbound(Class<? extends REFITMessage> type) {
		if (!inbounds.containsKey(type)) {
			inbounds.put(type, new REFITObserver<>());
		}
		inbounds.get(type).addListenersFrom(clientsInbound);
	}

	@Override
	public void receiveMessage(ByteBuffer message, short nodeID) {
		if (nodeID < REFITConfig.TOTAL_NR_OF_REPLICAS) {
			// Create message
			REFITMessage msg = REFITMessage.createMessage(message);
			if (REFITLogger.LOG_COMMUNICATION)
				REFITLogger.logCommunication(this, "received message " + message + " from " + msg.from);

			// Forward message
			receiveMessageFromReplica(msg);
		} else {
			// Create request
			REFITMessage request = REFITMessage.createMessage(message);
			if (request.type != REFITMessageType.REQUEST && request.type != REFITMessageType.REQUEST_WRAPPER) {
				REFITLogger.logWarning(this, "Dropping message of unexpected type from client " + nodeID + " " + request.type);
				return;
			}

			if (request.from == nodeID) {
				receiveMessageFromClient(request);
			} else {
				REFITLogger.logWarning(this, "Faked sender identity " + request + " " + request.from + " " + nodeID);
			}
		}
	}

	protected void receiveMessageFromReplica(REFITMessage message) {
		// check message type and then group type
		REFITObserver<REFITMessage> observer = inbounds.get(message.getClass());
		if (observer == null) {
			observer = inbounds.get(message.groupType());
		}
		if (observer != null) {
			observer.tell(message);
		} else {
			REFITLogger.logError(this, "drop message of unexpected type " + message.type);
		}
	}

	// ################
	// # START / STOP #
	// ################

	public void start() {
		// Create schedulers
		schedulers = new REFITScheduler[REFITConfig.NR_OF_REPLICA_SCHEDULERS];
		CountDownLatch startupSync = new CountDownLatch(REFITConfig.NR_OF_REPLICA_SCHEDULERS);
		REFITHangDetector hangDetector = (REFITConfig.TRACK_SCHEDULER_HANGS) ? new REFITHangDetector() : null;
		if (hangDetector != null) hangDetector.start();
		for (int i = 0; i < schedulers.length; i++) {
			schedulers[i] = new REFITScheduler(startupSync, hangDetector);
			schedulers[i].setName("RPLC" + id + "-" + i);
		}

		assignTasks(schedulers);

		group.assertAllTaskAreAssigned();

		// Mark as running
		REFITLogger.logEvent(this, "READY");

		// Start schedulers
		for (REFITScheduler scheduler : schedulers) scheduler.start();
	}

	public void stop() {
		for (REFITScheduler scheduler : schedulers) {
			scheduler.interrupt();
		}
		for (REFITScheduler scheduler : schedulers) {
			try {
				scheduler.join();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		network.shutdown();
	}
}
