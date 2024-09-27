package refit.application.counter;

import java.nio.ByteBuffer;
import java.util.Random;

import refit.client.REFITClientLibrary;
import refit.client.REFITClientTask;
import refit.config.REFITConfig;
import refit.message.REFITRequest.RequestMode;
import refit.util.REFITTime;
import refit.util.REFITIntervalStatistics.ResultType;

public class REFITCounterClientTask extends REFITClientTask {

	private final ByteBuffer request;
	private final ByteBuffer requestReadOnly;

	private int invocationCounter;
	private long invocationTime;
	private boolean readOnly;
	private final Random rand;
	private long lastRequestSeqNr;


	public REFITCounterClientTask(REFITClientLibrary service) {
		super(service);
		this.request = ByteBuffer.wrap(new byte[REFITConfig.REQUEST_SIZE]);
		this.requestReadOnly = ByteBuffer.wrap(new byte[(REFITConfig.REQUEST_SIZE > 0) ? REFITConfig.REQUEST_SIZE : 1]);
		this.requestReadOnly.put((byte) 0xff);
		this.requestReadOnly.position(0);

		this.invocationCounter = 0;
		this.invocationTime = -1L;

		rand = new Random(service.getNodeID());
		this.lastRequestSeqNr = -1;
	}


	@Override
	public String toString() {
		return "COCLNT[" + service.getNodeID() + "]";
	}


	// ##################
	// # SCHEDULER TASK #
	// ##################

	@Override
	protected void issueRequest() {
		// Issue next request
		invocationTime = REFITTime.nanoTime.getAsLong();
		readOnly = (rand.nextFloat() < REFITConfig.READ_WRITE_RATIO);
		service.invokeAsync((readOnly) ? requestReadOnly.slice() : request.slice(), lastRequestSeqNr,
				(readOnly) ? RequestMode.READ_WEAK : ((REFITConfig.WAIT_FOR_FULL_REPLY) ? RequestMode.WRITE : RequestMode.WRITE_ACK));
		if (!readOnly) invocationCounter++;
	}

	@Override
	protected boolean work() {
		if (invocationTime <= 0) {
			// no pending request
			return true;
		}

		// Receive stable result
		long now = REFITTime.nanoTime.getAsLong();
		service.getStatistics().event((long) ((now - invocationTime) / 1000f), service.getNodeID(), (readOnly) ? "r" : "w", ResultType.ACCEPT);
		if (!readOnly) lastRequestSeqNr = service.getAssignedSeqNr();
		event();
		invocationTime = 0;

		if (readOnly || REFITConfig.WAIT_FOR_FULL_REPLY) {
			// Check result
			ByteBuffer resultBuffer = service.getResult();
			int value = resultBuffer.getInt();
			if (value != invocationCounter) {
				throw new AssertionError("Bad result: counter value is " + value + " instead of " + invocationCounter);
			}
		}
		return true;
	}
}
