package refit.application.queue;

import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.Set;

import refit.application.REFITApplicationServer;
import refit.application.REFITResult;
import refit.message.REFITUniqueID;
import refit.replica.checkpoint.REFITCheckpointObject;

public class REFITQueueServer implements REFITApplicationServer {

	private final LinkedList<byte[]> queue;


	public REFITQueueServer() {
		this.queue = new LinkedList<>();
	}


	@Override
	public void init() {
		// Do nothing
	}

	@Override
	public boolean checkReadOnly(ByteBuffer payload) {
		return false;
	}

	@Override
	public REFITResult processRequest(REFITUniqueID uid, long agreementSeqNr, ByteBuffer payload) {
		REFITQueueRequest msg = new REFITQueueRequest(payload);

		REFITQueueReply reply;
		if (msg.isEnqueue) {
			queue.add(msg.data);
			reply = new REFITQueueReply();
		} else {
			int count = Math.min(queue.size(), msg.count);
			byte[][] data = new byte[count][];
			for (int i = 0; i < count; i++) {
				data[i] = queue.removeFirst();
			}

			reply = new REFITQueueReply(data);
		}

		reply.serializeMessage();

		// Return result
		return new REFITResult(reply.getBuffer());
	}

	@Override
	public String toString() {
		return "QUEUE";
	}

	@Override
	public REFITCheckpointObject[] createCheckpoint() {
		int len = Integer.BYTES;
		for (byte[] bytes : queue) {
			len += Short.BYTES + bytes.length;
		}

		ByteBuffer checkpoint = ByteBuffer.allocate(len);
		checkpoint.putInt(queue.size());
		for (byte[] bytes : queue) {
			checkpoint.putShort((short) bytes.length);
			checkpoint.put(bytes);
		}
		return new REFITCheckpointObject[]{new REFITCheckpointObject(checkpoint.array())};
	}

	@Override
	public void applyCheckpoint(final REFITCheckpointObject[] checkpoint) {
//		REFITLogger.logDebug(this, "Apply checkpoint");
		ByteBuffer checkpointBuffer = checkpoint[0].getBuffer();
		int queueLen = checkpointBuffer.getInt();
		queue.clear();
		for (int i = 0; i < queueLen; i++) {
			byte[] bytes = new byte[checkpointBuffer.getShort()];
			checkpointBuffer.get(bytes);
			queue.add(bytes);
		}
	}
}
