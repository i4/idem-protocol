package refit.application.counter;

import java.nio.ByteBuffer;
import java.util.Random;

import refit.client.REFITClientLibrary;
import refit.client.REFITClientThread;
import refit.client.REFITRejectException;
import refit.config.REFITConfig;
import refit.message.REFITRequest.RequestMode;
import refit.util.REFITLogger;
import refit.util.REFITTime;
import refit.util.REFITIntervalStatistics.ResultType;


public class REFITCounterClient extends REFITClientThread {

	private final ByteBuffer request;
	private final ByteBuffer requestReadOnly;
	private int invocationCounter;
	private final Random rand;
	private long lastRequestSeqNr;
	private int lastSeenCounter;


	public REFITCounterClient(REFITClientLibrary service) {
		super(service);
		this.request = ByteBuffer.wrap(new byte[REFITConfig.REQUEST_SIZE]);
		this.requestReadOnly = ByteBuffer.wrap(new byte[(REFITConfig.REQUEST_SIZE > 0) ? REFITConfig.REQUEST_SIZE : 1]);
		this.requestReadOnly.put((byte) 0xff);
		this.requestReadOnly.position(0);

		this.invocationCounter = 0;
		this.lastSeenCounter = 0;
		rand = new Random(service.getNodeID());
		this.lastRequestSeqNr = -1;
	}


	@Override
	protected boolean work() throws Exception {
		boolean readOnly = (rand.nextFloat() < REFITConfig.READ_WRITE_RATIO);
		ByteBuffer result = null;

		// Invoke operation
		long startTime = REFITTime.nanoTime.getAsLong();

		try {
		result = service.invoke((readOnly) ? requestReadOnly.slice() : request.slice(), lastRequestSeqNr,
				(readOnly) ? RequestMode.READ_WEAK : ((REFITConfig.WAIT_FOR_FULL_REPLY) ? RequestMode.WRITE : RequestMode.WRITE_ACK));
		} catch (REFITRejectException e) {
			// Request was rejected
			long endTime = REFITTime.nanoTime.getAsLong();
			service.getStatistics().event((long) ((endTime - startTime) / 1000f), service.getNodeID(), "reject", ResultType.ACCEPT);
			return true;
		}
		long endTime = REFITTime.nanoTime.getAsLong();
		service.getStatistics().event((long) ((endTime - startTime) / 1000f), service.getNodeID(), (readOnly) ? "r" : "w", ResultType.ACCEPT);

		if (!readOnly) {
			invocationCounter++;
			lastRequestSeqNr = service.getAssignedSeqNr();
		}

		if (readOnly || REFITConfig.WAIT_FOR_FULL_REPLY) {
			// Check result
			int value = result.getInt();

			if (lastSeenCounter - 10 > value)
				throw new Exception("Results are moving backwards got " + value + " expected at least " + lastSeenCounter);
			lastSeenCounter = value;
			if (!readOnly && value != invocationCounter)
				throw new Exception("Bad result: counter value is " + value + " instead of " + invocationCounter);
		}
		return false;
	}
}
