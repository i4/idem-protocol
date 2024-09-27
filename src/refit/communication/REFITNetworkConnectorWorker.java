package refit.communication;

import java.io.IOException;
import java.net.ConnectException;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;

import refit.config.REFITConfig;
import refit.message.REFITConnect;
import refit.message.REFITUniqueID;
import refit.scheduler.REFITSchedulerGroup;
import refit.scheduler.REFITSchedulerTask;
import refit.scheduler.REFITTimer;
import refit.util.REFITStackTrace;


public class REFITNetworkConnectorWorker extends REFITSchedulerTask {

	enum ConnectionState {
		// the other side initiated the connection
		WAIT_FOR_ID,
		// we initiated the connection
		WAIT_FOR_CONNECT,
		WAIT_FOR_ACK,
		// shared
		DISCONNECTED
	}

	private static final int CONNECTION_TIMEOUT = 5 * 1000;

	private final REFITNetworkEndpoint endpoint;
	private final REFITConnection connection;
	private final String instanceName;
	private final short myNodeID;

	private boolean initCalled;
	private ConnectionState state;
	// we initiated the connection if the SocketAddress inside address != null
	private REFITNodeAddress address;
	private REFITTimer timer;


	public REFITNetworkConnectorWorker(REFITSchedulerGroup group, REFITNetworkEndpoint endpoint, short myNodeID, int workerCounter) {
		// always use the same taskType as the endpoint to avoid threading problems
		super(endpoint.taskType, group);
		this.endpoint = endpoint;
		this.connection = new REFITConnection(REFITConnect.MSG_SIZE, 0, REFITUnmarshaller.instance, (short) -1);
		this.initCalled = false;
		this.instanceName = String.format("CONWK%02d", workerCounter);

		this.myNodeID = myNodeID;
		state = ConnectionState.DISCONNECTED;
	}

	@Override
	public String toString() {
		return instanceName;
	}

	public void init(SocketChannel socketChannel) {
		if (!state.equals(ConnectionState.DISCONNECTED)) {
			throw new IllegalStateException("Trying to initialize a worker twice.");
		}
		timer = newTimer(this::timeout);
		address = null;
		try {
			connection.init(socketChannel);
		} catch (IOException e) {
			abort(REFITStackTrace.toString(e));
			return;
		}
		initCalled = true;
		state = ConnectionState.WAIT_FOR_ID;
		// timeout for connection setup
		timer.start(CONNECTION_TIMEOUT);
		progress();
	}

	public void connect(REFITNodeAddress address) {
		if (!state.equals(ConnectionState.DISCONNECTED)) {
			throw new IllegalStateException("Trying to initialize a worker twice.");
		}
		timer = newTimer(this::timeout);
		this.address = address;
		try {
			connection.connect(address.address);
		} catch (IOException e) {
			abort(REFITStackTrace.toString(e));
			return;
		}
		initCalled = true;
		state = ConnectionState.WAIT_FOR_CONNECT;
		// timeout for connection setup
		timer.start(CONNECTION_TIMEOUT);
		progress();
	}

	public void shutdown() {
		if (connection.isOpen()) {
			connection.closeConnection();
		}
		initCalled = false;
		address = null;
		state = ConnectionState.DISCONNECTED;
		timer.clear();
	}

	private void abort(String errorMessage) {
		REFITNodeAddress address = this.address;
		shutdown();
		endpoint.abortConnection(this, null, address, errorMessage);
	}

	private void complete() {
		if (!connection.isOpen()) {
			abort("connection disappeared");
			return;
		}
		SocketChannel channel = connection.takeChannel();
		REFITNodeAddress address = this.address;
		shutdown();
		endpoint.finishConnection(this, channel, address);
	}

	// ##################
	// # SCHEDULER TASK #
	// ##################

	@Override
	public void selectKey(SelectionKey key) {
		if (REFITConfig.ENABLE_DEBUG_CHECKS && key != connection.selectionKey) {
			throw new IllegalArgumentException("Called for wrong selection key!");
		}
		connection.select();
	}

	private void timeout() {
		// timeout during connection setup
		if (!state.equals(ConnectionState.DISCONNECTED)) {
			abort("timeout");
		}
	}

	@Override
	public void execute() {
		if (state.equals(ConnectionState.DISCONNECTED)) return;

		if (initCalled) {
			// register() should only be called from the thread that owns the selector
			// otherwise registering a selectionKey may deadlock
			connection.register(this);
			initCalled = false;
		}

		if (state.equals(ConnectionState.WAIT_FOR_CONNECT)) {
			boolean isConnected;
			try {
				isConnected = connection.connect();
			} catch (IOException e) {
				String err;
				if (e instanceof ConnectException || e.getMessage().equals("Connection reset by peer")) {
					err = "connection failed: " + e;
				} else {
					// something went horribly wrong, close the connection
					err = REFITStackTrace.toString(e);
				}
				abort(err);
				return;
			}
			if (isConnected) {
//				REFITLogger.logEvent(this, "connected to " + this.address + " by " + myNodeID + " via " + connection);
				REFITConnect connect = new REFITConnect(new REFITUniqueID(myNodeID, REFITConnect.CONNECT_SEQNR), (short) -1);
				connect.serializeMessage();
				connection.enqueue(connect.getBuffer());

				state = ConnectionState.WAIT_FOR_ACK;
			}
		}

		// Handle sending
		connection.send();

		// Handle receiving
		connection.receive(message -> {
			event();
			switch (state) {
			case WAIT_FOR_ID: {
				REFITConnect connect = new REFITConnect(message);
				if (connect.uid.seqNr != REFITConnect.CONNECT_SEQNR) {
					abort("invalid id handshake from " + connection);
					return;
				}
				short otherNodeID = connect.uid.nodeID;
				// generate pseudo address
				address = new REFITNodeAddress(otherNodeID, null);

				if (!endpoint.isNodeAllowedToConnect(otherNodeID)) {
					abort("dropping connection from " + otherNodeID);
					return;
				}

				REFITConnect ack = new REFITConnect(new REFITUniqueID(otherNodeID, REFITConnect.ACCEPT_SEQNR), (short) -1);
				ack.serializeMessage();
				connection.enqueue(ack.getBuffer());
				// Assume that the call to send will always succeed, writing 17 bytes should be always possible as
				// the standard minimum window size is 4kB
				connection.send();

				complete();
				return;
			}
			case WAIT_FOR_ACK: {
				REFITConnect connect = new REFITConnect(message);
				if (connect.uid.seqNr != REFITConnect.ACCEPT_SEQNR) {
					abort("invalid ack handshake from " + connection);
					return;
				}
				if (connect.uid.nodeID != myNodeID) {
					abort("mismatched client connection!!! got reply for " + connect.uid.nodeID + " " + connection);
					return;
				}
				complete();
				return;
			}
			default:
				throw new InternalError("Connection state tracking is broken!");
			}
		});

		if (state != ConnectionState.DISCONNECTED && !connection.isOpen()) {
			// something went horribly wrong, close the connection
			abort("connection was closed");
		}
	}
}
