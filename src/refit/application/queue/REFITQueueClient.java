package refit.application.queue;

import java.nio.ByteBuffer;

import refit.client.REFITClientLibrary;
import refit.client.REFITClientThread;
import refit.config.REFITConfig;
import refit.message.REFITRequest;
import refit.util.REFITTime;
import refit.util.REFITIntervalStatistics.ResultType;


public class REFITQueueClient extends REFITClientThread {

	private long lastRequestSeqNr;


	public REFITQueueClient(REFITClientLibrary service) {
		super(service);
		this.lastRequestSeqNr = -1;
	}


	@Override
	protected boolean work() throws Exception {
		// multiply client id with ratio and check if the part before the point changes
		// this provides a nice approximate client job distribution
		// e.g. ratio = 0.4 -> 0, 0.4, 0.8, 1.2, 1.6, 2.0 -> the third and fifth client are selected
		int isSkip = (int) (REFITConfig.READ_WRITE_RATIO * (1 + service.getNodeID())) - (int) (REFITConfig.READ_WRITE_RATIO * service.getNodeID());
		boolean isEnqueue = isSkip < 1;

		if (isEnqueue) {
			enqueue();
		} else {
			dequeue();
		}
		return false;
	}

	private void enqueue() throws Exception {
		byte[] data = new byte[REFITConfig.REQUEST_SIZE];
		REFITQueueRequest msg = new REFITQueueRequest(data);
		msg.serializeMessage();

		long startTime = REFITTime.nanoTime.getAsLong();
		service.invoke(msg.getBuffer(), lastRequestSeqNr, REFITRequest.RequestMode.WRITE_ACK);
		long endTime = REFITTime.nanoTime.getAsLong();
		service.getStatistics().event((long) ((endTime - startTime) / 1000f), service.getNodeID(), "e", ResultType.ACCEPT);
		lastRequestSeqNr = service.getAssignedSeqNr();
	}

	private void dequeue() throws Exception {
		REFITQueueRequest msg = new REFITQueueRequest((short) 10);
		msg.serializeMessage();

		// Invoke operation
		long startTime = REFITTime.nanoTime.getAsLong();
		ByteBuffer reply = service.invoke(msg.getBuffer(), lastRequestSeqNr, REFITRequest.RequestMode.WRITE);
		long endTime = REFITTime.nanoTime.getAsLong();
		service.getStatistics().event((long) ((endTime - startTime) / 1000f), service.getNodeID(), "d", ResultType.ACCEPT);
		lastRequestSeqNr = service.getAssignedSeqNr();

		REFITQueueReply entry = new REFITQueueReply(reply);

		if (entry.isEnqueue)
			throw new Exception("Bad result: Expected response to dequeue, got enqueue");

//		REFITLogger.logWarning(this, "Got number of entries " + entry.data.length);
	}
}
