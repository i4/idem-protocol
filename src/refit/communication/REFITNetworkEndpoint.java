package refit.communication;

import java.io.IOException;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.Function;

import refit.communication.REFITNetworkEndpointWorker.ConnectionParameters;
import refit.communication.REFITNetworkEvents.REFITConnectionWriteableNotification;
import refit.communication.REFITNetworkEvents.REFITInitialConnectionsCompleted;
import refit.config.REFITConfig;
import refit.message.REFITMessage;
import refit.scheduler.REFITSchedulerFacade;
import refit.scheduler.REFITSchedulerTask;
import refit.scheduler.REFITSchedulerTaskType;
import refit.scheduler.REFITTimer;
import refit.stage.REFITEventReceiver;
import refit.stage.REFITObserver;
import refit.util.REFITLogger;
import refit.util.REFITTime;


public class REFITNetworkEndpoint extends REFITSchedulerTask {

	public final REFITObserver<REFITInitialConnectionsCompleted> initCompletedSender;
	private final short myNodeID;
	private final boolean delayInitialConnects;

	private Set<Short> initialNodes;
	private final boolean permanentNodeRestriction;
	private final List<REFITNodeAddress> initialConnects;
	private final Set<Short> connectedNodes;

	private final ServerSocketChannel serverSocketChannel;
	private SelectionKey key;
	private final Queue<REFITNodeAddress> connectQueue;
	private final PriorityQueue<DelayedAddress> delayedConnectQueue;
	private final Deque<REFITNetworkConnectorWorker> inactiveConnectors;
	private final Set<REFITNetworkConnectorWorker> pendingConnectors;
	private final REFITNetworkEndpointWorker[] workers;

	private final int connectorsCount;
	private REFITTimer timer;


	public REFITNetworkEndpoint(REFITNetworkInboundRouter replica, int workerCount, Function<Short, ConnectionParameters> workerConfigFunc, int connectorsCount,
	                            Set<Short> initialNodes, boolean permanentNodeRestriction, List<REFITNodeAddress> initialConnects, boolean createServerSocket, boolean delayInitialConnects) {
		super(REFITSchedulerTaskType.NETWORK_ENDPOINT, replica.getGroup());
		this.myNodeID = replica.getNodeID();
		this.delayInitialConnects = delayInitialConnects;

		initCompletedSender = new REFITObserver<>();

		connectQueue = new ConcurrentLinkedQueue<>();
		delayedConnectQueue = new PriorityQueue<>();
		inactiveConnectors = new ArrayDeque<>();
		pendingConnectors = new HashSet<>();
		workers = new REFITNetworkEndpointWorker[workerCount];

		for (short i = 0; i < workers.length; i++) {
			workers[i] = new REFITNetworkEndpointWorker(replica, this, myNodeID, i, workerConfigFunc.apply(i));
		}
		this.connectorsCount = connectorsCount;
		for (int i = 0; i < connectorsCount; i++) {
			inactiveConnectors.add(new REFITNetworkConnectorWorker(replica.getGroup(), this, myNodeID, i));
		}

		this.initialNodes = initialNodes;
		this.permanentNodeRestriction = permanentNodeRestriction;
		this.initialConnects = initialConnects;
		this.connectedNodes = new HashSet<>();

		ServerSocketChannel channel = null;

		if (createServerSocket) {
			try {
				channel = ServerSocketChannel.open();
				channel.configureBlocking(false);
			} catch (IOException e) {
				e.printStackTrace();
				System.exit(1);
			}
		}
		serverSocketChannel = channel;
	}


	@Override
	public String toString() {
		return "RPNET";
	}


	// ############################
	// # ESTABLISHING CONNECTIONS #
	// ############################


	@Override
	public void init() {
		timer = newTimer(this::timeout);
		// Create server socket channel
		if (serverSocketChannel != null) {
			try {
				serverSocketChannel.setOption(StandardSocketOptions.SO_REUSEADDR, true);
				for (int i = 0; true; i++) {
					try {
						serverSocketChannel.socket().bind(REFITConfig.BIND_ADDRESSES[myNodeID], REFITConfig.SERVER_SOCKET_BACKLOG);
						break;
					} catch (IOException ioe) {
						if (i == 5) {
							throw ioe;
						}
						try {
							REFITTime.sleep(500);
						} catch (InterruptedException ignored) {
						}
					}
				}
			} catch (IOException ioe) {
				ioe.printStackTrace();
				System.exit(1);
			}
		}

		if (!delayInitialConnects) {
			connectQueue.addAll(initialConnects);
		}
		progress();
	}

	public void completeInitialConnects() {
		if (!delayInitialConnects || !connectQueue.isEmpty()) {
			throw new IllegalStateException("Most only be called when the initial connects are delayed");
		}
		connectQueue.addAll(initialConnects);
		progress();
	}

	public void shutdown() {
		// not really well synchronized, but good enough for REFITLocalSystem
		for (REFITNetworkEndpointWorker worker : workers) {
			if (worker != null) {
				worker.shutdown();
			}
		}
		for (REFITNetworkConnectorWorker worker : pendingConnectors) {
			worker.shutdown();
		}
		if (serverSocketChannel != null) {
			try {
				serverSocketChannel.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	@Override
	public void setScheduler(REFITSchedulerFacade scheduler) {
		super.setScheduler(scheduler);
		if (serverSocketChannel != null) {
			try {
				key = registerIO(serverSocketChannel, SelectionKey.OP_ACCEPT);
			} catch (IOException ioe) {
				ioe.printStackTrace();
			}
		}
	}

	@Override
	public void selectKey(SelectionKey key) {
		// just ignore it and try to accept a new connection when execute is called
	}

	@Override
	public void execute() {
		// Leave some connectors to accept incoming connections
		int minAcceptConnectors = (serverSocketChannel == null) ? 0 : connectorsCount / 4;
		while (!connectQueue.isEmpty() && inactiveConnectors.size() > minAcceptConnectors) {
			REFITNodeAddress address = connectQueue.poll();
//			REFITLogger.logDebug(this, "Connecting to node " + address);
			REFITNetworkConnectorWorker worker = inactiveConnectors.removeFirst();
			pendingConnectors.add(worker);
			worker.connect(address);
		}

		if (serverSocketChannel == null) {
			return;
		}

		while (true) {
			if (inactiveConnectors.isEmpty()) {
//				REFITLogger.logError(this, "Out of workers");
				// no need to receive connect events
				key.interestOps(0);
				break;
			}

			SocketChannel channel = null;
			try {
				// Accept connection or return null if none is pending
				channel = serverSocketChannel.accept();
			} catch (IOException ioe) {
				ioe.printStackTrace();
			}
			if (channel == null) break;

			REFITNetworkConnectorWorker worker = inactiveConnectors.removeFirst();
			pendingConnectors.add(worker);
			worker.init(channel);
			event();
		}
	}

	public void abortConnection(REFITNetworkConnectorWorker worker, SocketChannel channel, REFITNodeAddress address, String errorMessage) {
		if (!pendingConnectors.contains(worker)) {
			throw new IllegalStateException("Can only disconnect pending workers");
		}
		if (channel != null) {
			deregisterIO(channel);
		}

//		REFITLogger.logDebug(this, "Connection to node " + address + " failed");
		releaseWorker(worker);

		queueReconnect(address, errorMessage);
	}

	private void releaseWorker(REFITNetworkConnectorWorker worker) {
		pendingConnectors.remove(worker);
		inactiveConnectors.add(worker);
		// we've got at least one waiting connector
		if (key != null) {
			key.interestOps(SelectionKey.OP_ACCEPT);
		}
		progress();
	}

	public boolean isNodeAllowedToConnect(short nodeID) {
		// check whether client is allowed to connect
		if (nodeID < 0 || nodeID >= workers.length || nodeID == myNodeID) {
			return false;
		}
		// by not checking whether a client connection still exists, this allows a client to replace its connection
		return (initialNodes == null || initialNodes.contains(nodeID));
	}

	public void finishConnection(REFITNetworkConnectorWorker worker, SocketChannel channel, REFITNodeAddress address) {
		if (!pendingConnectors.contains(worker)) {
			throw new IllegalStateException("Can only activate pending workers");
		}
		short nodeID = address.nodeID;
		if (!isNodeAllowedToConnect(nodeID)) {
			throw new InternalError("Shouldn't connect to disallowed nodes");
		}

		releaseWorker(worker);

		// cancel selection key if the channel is moved between schedulers
		REFITNetworkEndpointWorker connectionWorker = workers[nodeID];
		if (!sameScheduler(connectionWorker)) {
			deregisterIO(channel);
		}

		if (connectionWorker.init(channel, address)) {
			connectedNodes.add(nodeID);
		} else {
			// failed to complete connection setup
			queueReconnect(address, "failed to move connection");
		}

		if (!permanentNodeRestriction && connectedNodes.equals(initialNodes)) {
			// all initial nodes have connected, lift restrictions
			initialNodes = null;
//			REFITLogger.logEvent(this, "Completed initial node connections");

			initCompletedSender.tell(new REFITInitialConnectionsCompleted(myNodeID));
		}
	}

	public void queueReconnect(REFITNodeAddress address, String errorMessage) {
		if (address != null && address.address != null) {
			REFITLogger.logWarning(this, "Reconnecting to " + address + " by " + myNodeID + ": " + errorMessage);
			// slightly delay reconnection
			queueAddressDelay(address, 500);
			progress();
		} else {
			REFITLogger.logWarning(this, "Lost connection to " + address + " by " + myNodeID + ": " + errorMessage);
		}
	}

	// #############
	// # MULTICAST #
	// #############

	public void unicastMessage(REFITMessage message, short nodeID) {
//		if (REFITLogger.LOG_COMMUNICATION)
//			REFITLogger.logCommunication(this, "send message " + message + " to " + nodeID);
		if (!message.isVerified()) {
			throw new AssertionError("Trying to send an unverified message");
		}

		ByteBuffer buffer = message.getBuffer();
		if (workers[nodeID].isOpen()) {
			workers[nodeID].enqueueMessage(buffer);
		} else {
			if (REFITLogger.LOG_COMMUNICATION)
				REFITLogger.logCommunication(this, "Trying to send message to unavailable node " + nodeID);
		}
	}

	// return false if enqueueing has failed
	public boolean unicastMessageBounded(REFITMessage message, short nodeID, REFITEventReceiver<? super REFITConnectionWriteableNotification> stage) {
//		if (REFITLogger.LOG_COMMUNICATION)
//			REFITLogger.logCommunication(this, "send message " + message + " to " + nodeID);
		if (!message.isVerified()) {
			throw new AssertionError("Trying to send an unverified message");
		}

		ByteBuffer buffer = message.getBuffer();
		boolean success = workers[nodeID].boundedEnqueueMessage(buffer.slice(), stage);

//		if (REFITLogger.LOG_COMMUNICATION && !workers[nodeID].isOpen()) {
//			// no connection to replica -> failed enqueue
//			REFITLogger.logCommunication(this, "Trying to send message to unavailable node " + nodeID);
//		}
		return success;
	}


	private void queueAddressDelay(REFITNodeAddress address, long delay) {
		DelayedAddress oldHead = delayedConnectQueue.peek();

		long delayUntil = REFITTime.currentTimeMillis.getAsLong() + delay;
		delayedConnectQueue.add(new DelayedAddress(address, delayUntil));

		DelayedAddress newHead = delayedConnectQueue.peek();
		if (newHead != oldHead) {
			// update timeout
			timer.startAbsolute(newHead.delayUntil);
		}
	}

	private void timeout() {
		long time = REFITTime.currentTimeMillis.getAsLong();
		boolean madeProgress = false;
		while (!delayedConnectQueue.isEmpty() && delayedConnectQueue.peek().delayUntil <= time) {
			DelayedAddress delayedAddress = delayedConnectQueue.poll();
			connectQueue.add(delayedAddress.address);
			madeProgress = true;
		}
		if (madeProgress) {
			// must manually wakeup ourselves as the timer doesn't do that
			progress();
		}

		if (!delayedConnectQueue.isEmpty()) {
			timer.startAbsolute(delayedConnectQueue.peek().delayUntil);
		}
	}

	private static class DelayedAddress implements Comparable<DelayedAddress> {
		public final REFITNodeAddress address;
		public final long delayUntil;

		public DelayedAddress(REFITNodeAddress address, long delayUntil) {
			this.address = address;
			this.delayUntil = delayUntil;
		}

		@Override
		public int compareTo(DelayedAddress o) {
			return Long.compare(delayUntil, o.delayUntil);
		}

		@Override
		public String toString() {
			return "{" + address + ", " + delayUntil + '}';
		}
	}
}
