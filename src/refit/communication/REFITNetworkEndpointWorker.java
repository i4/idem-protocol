package refit.communication;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.util.function.Consumer;

import refit.communication.REFITNetworkEvents.REFITConnectionWriteableNotification;
import refit.config.REFITConfig;
import refit.scheduler.REFITSchedulerTask;
import refit.scheduler.REFITSchedulerTaskType;
import refit.stage.REFITEventReceiver;


public class REFITNetworkEndpointWorker extends REFITSchedulerTask {

	private final REFITNetworkInboundRouter replica;
	private final REFITNetworkEndpoint endpoint;
	private final REFITConnection connection;
	private SelectionKey oldKey;
	private final String instanceName;
	private final short nodeID;
	private REFITNodeAddress address;

	private boolean initCalled;

	public REFITNetworkEndpointWorker(REFITNetworkInboundRouter replica, REFITNetworkEndpoint endpoint, short replicaID, short nodeID, ConnectionParameters parameters) {
		super(parameters.taskType, replica.getGroup());
		this.replica = replica;
		this.endpoint = endpoint;
		this.nodeID = nodeID;
		this.connection = new REFITConnection(parameters.receiveBufferSize, parameters.sendBufferSize, REFITUnmarshaller.instance, nodeID);
		this.initCalled = false;
		this.instanceName = String.format("NWK%02d", nodeID);
	}


	@Override
	public String toString() {
		return instanceName;
	}

	public boolean init(SocketChannel socketChannel, REFITNodeAddress address) {
		oldKey = connection.selectionKey;
		if (connection.isOpen()) {
			connection.closeConnection();
		}

		if (address.nodeID != nodeID) {
			throw new IllegalArgumentException("Got connection for wrong node!");
		}
		this.address = address;
		try {
			connection.init(socketChannel);
		} catch (IOException e) {
			e.printStackTrace();
			return false;
		}
		initCalled = true;

		progress();
		return true;
	}

	public boolean isOpen() {
		return connection.isOpen();
	}

	// ##################
	// # SCHEDULER TASK #
	// ##################

	@Override
	public void selectKey(SelectionKey key) {
		if (key == oldKey) {
			return;
		}
		if (REFITConfig.ENABLE_DEBUG_CHECKS && key != connection.selectionKey) {
			throw new IllegalArgumentException("Called for wrong selection key!");
		}
		connection.select();
	}

	private final Consumer<ByteBuffer> messageHandler = this::handleMessage;

	private void handleMessage(ByteBuffer message) {
		// Forward request
		replica.receiveMessage(message, nodeID);
		event();
	}

	@Override
	public void execute() {
		if (initCalled) {
			// register() should only be called from the thread that owns the selector
			// otherwise registering a selectionKey may deadlock
			connection.register(this);
			initCalled = false;
		}

		// Handle sending
		connection.send();

		// Handle receiving
		connection.receive(messageHandler);

		if (!connection.isOpen() && address != null) {
			REFITNodeAddress address = this.address;
			shutdown();
			endpoint.queueReconnect(address, "connection closed");
		}
	}

	// ###########
	// # SENDING #
	// ###########

	public void enqueueMessage(ByteBuffer message) {
		connection.enqueue(message);
		progress();
	}

	public boolean boundedEnqueueMessage(ByteBuffer message, REFITEventReceiver<? super REFITConnectionWriteableNotification> stage) {
		boolean success = connection.boundedEnqueue(message, stage);
		progress();
		return success;
	}

	public void shutdown() {
		if (connection.isOpen()) {
			connection.closeConnection();
		}
		address = null;
	}

	public static class ConnectionParameters {
		public final REFITSchedulerTaskType taskType;
		public final int receiveBufferSize;
		public final int sendBufferSize;

		public ConnectionParameters(REFITSchedulerTaskType taskType, int receiveBufferSize, int sendBufferSize) {
			this.taskType = taskType;
			this.receiveBufferSize = receiveBufferSize;
			this.sendBufferSize = sendBufferSize;
		}
	}
}
