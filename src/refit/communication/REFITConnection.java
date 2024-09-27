package refit.communication;

import java.io.IOException;
import java.net.SocketAddress;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.util.HashSet;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import refit.communication.REFITNetworkEvents.REFITConnectionWriteableNotification;
import refit.scheduler.REFITSchedulerTask;
import refit.stage.REFITEventReceiver;
import refit.util.REFITLogger;


public class REFITConnection {

	private final REFITUnmarshaller unmarshaller;

	// use volatile to make sure that isOpen() can return a somewhat reasonable result
	private volatile SocketChannel socketChannel;
	private SocketAddress socketAddress;
	public SelectionKey selectionKey;
	private boolean isReadable;
	private boolean isWriteable;
	private boolean isConnectable;
	private final short id;


	public REFITConnection(int receiveBufferSize, int sendBufferSize, REFITUnmarshaller unmarshaller, short nodeID) {
		this.unmarshaller = unmarshaller;
		this.receiveBuffer = ByteBuffer.allocateDirect(receiveBufferSize);
		this.boundedSendQueue = new ConcurrentLinkedQueue<>();
		this.boundedSendQueueBytes = new AtomicInteger(0);
		this.waitingForSend = new HashSet<>();
		// FIXME: eventually get rid of sendQueue. However, this requires potentially large modifications to every single stage
		this.sendQueue = new ConcurrentLinkedQueue<>();
		this.useStaticSendBuffer = (sendBufferSize > 0);
		this.staticSendBuffer = useStaticSendBuffer ? ByteBuffer.allocateDirect(sendBufferSize) : null;
		this.boundedSendQueueLimit = (sendBufferSize > 0) ? sendBufferSize / 2 : 10 * 1024;
		this.sendBuffer = null;
		this.id = nodeID;
	}

	@Override
	public String toString() {
		Object socket = (socketChannel != null) ? socketChannel.socket() : null;
		return "CNCTN[" + socket + "]";
	}


	public void init(SocketChannel socketChannel) throws IOException {
		if (socketChannel == null) throw new IllegalArgumentException();
		// Reset helper data structures
		this.socketChannel = socketChannel;
		selectionKey = null;
		socketAddress = null;

		// wakeup every waiting outbox
		sendWakeups();
		waitingForSend.clear();
		hasWaitingForSend = false;

		// reset send and receive queues
		boundedSendQueue.clear();
		boundedSendQueueBytes.set(0);
		sendQueue.clear();
		sendPending = false;
		sendBuffer = null;
		receiveBuffer.clear();
		receiveProcessedMark = 0;

		isReadable = true;
		isWriteable = true;
		isConnectable = true;

		try {
			// Configure channel
			socketChannel.configureBlocking(false);
			socketChannel.setOption(StandardSocketOptions.TCP_NODELAY, true);
		} catch (IOException e) {
			closeConnection();
			throw e;
		}
	}

	public void connect(SocketAddress address) throws IOException {
		init(SocketChannel.open());
		socketAddress = address;
		try {
			socketChannel.connect(address);
		} catch (IOException e) {
			closeConnection();
			throw e;
		}
		isReadable = false;
		isWriteable = false;
		// test immediately whether the connection can be completed
		// without this, the connection setup seems to hang at least on localhost
		isConnectable = true;
	}


	public void register(REFITSchedulerTask task) {
		try {
			// Register channel for I/O operations
			selectionKey = task.registerIO(socketChannel, (socketAddress == null) ? SelectionKey.OP_READ : SelectionKey.OP_CONNECT);
		} catch (IOException ioe) {
			closeConnection();
		}
	}

	public void select() {
		isReadable = selectionKey.isReadable();
		isWriteable = (selectionKey.interestOps() & SelectionKey.OP_WRITE) == 0 || selectionKey.isWritable();
		isConnectable = selectionKey.isConnectable();
	}

	public boolean isOpen() {
		return (socketChannel != null);
	}

	public void closeConnection() {
		if (selectionKey != null) selectionKey.cancel();
		try {
			socketChannel.close();
		} catch (IOException ignored) {
		}
		socketChannel = null;
		selectionKey = null;
	}

	public SocketChannel takeChannel() {
		SocketChannel channel = socketChannel;
		selectionKey = null;
		socketChannel = null;
		socketAddress = null;
		return channel;
	}


	// ###########
	// # SENDING #
	// ###########

	private final boolean useStaticSendBuffer;
	private final ByteBuffer staticSendBuffer;

	private final Queue<ByteBuffer> boundedSendQueue;
	private final AtomicInteger boundedSendQueueBytes;
	private final int boundedSendQueueLimit;
	private final Set<REFITEventReceiver<? super REFITConnectionWriteableNotification>> waitingForSend;
	private volatile boolean hasWaitingForSend;

	private final Queue<ByteBuffer> sendQueue;
	private ByteBuffer sendBuffer;
	private boolean sendPending;


	public void enqueue(ByteBuffer message) {
		if (!isOpen()) return;
		sendQueue.add(message);
	}

	public boolean connect() throws IOException {
		if (!isConnectable) return false;

		boolean isConnected = socketChannel.finishConnect();
		if (!isConnected) {
			// connection not yet complete
			return false;
		}

		selectionKey.interestOps(selectionKey.interestOps() & ~SelectionKey.OP_CONNECT | SelectionKey.OP_READ);
		return true;
	}

	private boolean isBoundedQueueFull() {
		return boundedSendQueueBytes.get() > boundedSendQueueLimit;
	}

	private boolean isBoundedQueueBusy() {
		return boundedSendQueueBytes.get() > boundedSendQueueLimit / 2;
	}

	public boolean boundedEnqueue(ByteBuffer message, REFITEventReceiver<? super REFITConnectionWriteableNotification> stage) {
		if (!isOpen() || isBoundedQueueFull() || hasWaitingForSend) {
			synchronized (waitingForSend) {
				hasWaitingForSend = true;
				waitingForSend.add(stage);
			}
			return false;
		}
		boundedSendQueue.add(message);
		boundedSendQueueBytes.addAndGet(message.remaining());
		return true;
	}

	private void sendWakeups() {
		if (hasWaitingForSend) {
			synchronized (waitingForSend) {
				for (REFITEventReceiver<? super REFITConnectionWriteableNotification> stage : waitingForSend) {
					stage.tell(new REFITConnectionWriteableNotification(id));
				}
				waitingForSend.clear();
				hasWaitingForSend = false;
			}
		}
	}

	// TODO: Single send operation for retry and new data when using a static send buffer
	public void send() {
		// Check whether connection is open
		if (!isOpen()) return;

		// Try to complete pending send
		if (sendPending) {
			if (!isWriteable) return;
			boolean success = trySend();
			if (!success) return;
		}

		// Prepare send buffer
		while (true) {
			ByteBuffer queueHead = boundedSendQueue.poll();
			if (queueHead == null) {
				queueHead = sendQueue.poll();
			} else {
				boundedSendQueueBytes.addAndGet(-queueHead.remaining());
			}
			if (queueHead == null) break;
			// Only fallback to message buffer, if message is too large
			if (useStaticSendBuffer && staticSendBuffer.remaining() >= queueHead.remaining()) {

				sendBuffer = staticSendBuffer;
				sendBuffer.put(queueHead);
				while (!boundedSendQueue.isEmpty() || !sendQueue.isEmpty()) {
					boolean fromBoundedQueue = true;
					ByteBuffer message = boundedSendQueue.peek();
					if (message == null) {
						message = sendQueue.peek();
						fromBoundedQueue = false;
					}
					if (sendBuffer.remaining() < message.remaining()) break;
					if (fromBoundedQueue) {
						boundedSendQueue.poll();
						boundedSendQueueBytes.addAndGet(-message.remaining());
					} else {
						sendQueue.poll();
					}
					sendBuffer.put(message);
				}
				sendBuffer.flip();
			} else {
				sendBuffer = queueHead;
			}

			// Try to send data
			boolean success = trySend();
			if (!success) break;
		}

		if (!isBoundedQueueBusy()) {
			sendWakeups();
		}
	}

	private boolean trySend() {
		try {
			// Try to send data
			int bytesToSend = sendBuffer.remaining();
			int bytesSent = socketChannel.write(sendBuffer);
			sendPending = (bytesSent < bytesToSend);
			if (sendPending) {
				//REFITLogger.logWarning(this, "write stalled (" + socketChannel.socket().getRemoteSocketAddress() + "): only " + bytesSent + " of " + bytesToSend + " bytes sent");
				selectionKey.interestOps(selectionKey.interestOps() | SelectionKey.OP_WRITE);
				isWriteable = false;
				return false;
			}

			// Sending has been successful
			if (sendBuffer == staticSendBuffer) sendBuffer.clear();
			else sendBuffer = null;
			selectionKey.interestOps(selectionKey.interestOps() & ~SelectionKey.OP_WRITE);
			return true;
		} catch (IOException ioe) {
			REFITLogger.logWarning(this, "Broken connection");
			closeConnection();
			return false;
		}
	}


	// #############
	// # RECEIVING #
	// #############

	private final ByteBuffer receiveBuffer;
	private int receiveProcessedMark;


	public void receive(Consumer<ByteBuffer> messageHandler) {
		// Check whether connection is open and data is available
		if (!isOpen()) return;
		if (!isReadable) return;

		// Compact buffer if it is almost full
		if (receiveBuffer.position() > (receiveBuffer.capacity() * 3 / 4)) {
			receiveBuffer.limit(receiveBuffer.position());
			receiveBuffer.position(receiveProcessedMark);
			receiveProcessedMark = 0;
			receiveBuffer.compact();
		}

		// Get data
		int bytesRead;
		try {
			// Receive new data
			bytesRead = socketChannel.read(receiveBuffer);
		} catch (IOException ioe) {
			bytesRead = -1;
		}

		// Close connection in case of an exception or EOF
		if (bytesRead < 0) {
			closeConnection();
			return;
		}

		// Prepare buffer for unmarshalling
		int dataEndPosition = receiveBuffer.position();
		receiveBuffer.limit(dataEndPosition);
		receiveBuffer.position(receiveProcessedMark);

		// Unmarshal messages
		boolean receivedAMessage = false;
		while (receiveBuffer.hasRemaining()) {
			boolean foundMessage = unmarshaller.unmarshalMessage(receiveBuffer, messageBuffer -> {
				final ByteBuffer messageCopy = ByteBuffer.allocate(messageBuffer.remaining());
				messageCopy.put(messageBuffer);
				messageCopy.flip();
				messageHandler.accept(messageCopy.asReadOnlyBuffer());
			});
			if (!foundMessage) break;
			receivedAMessage = true;
		}

		// Update processed mark and reset buffer
		receiveProcessedMark = receiveBuffer.position();
		receiveBuffer.limit(receiveBuffer.capacity());
		receiveBuffer.position(dataEndPosition);

		isReadable = false;

		// Message is too large for the receive buffer
		if (receiveProcessedMark == 0 && receiveBuffer.remaining() == 0 && !receivedAMessage) {
			REFITLogger.logWarning(this, "Message too large to receive, buffer size " + receiveBuffer.capacity());
			closeConnection();
		}
	}
}
