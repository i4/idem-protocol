package refit.application.log;

import java.nio.ByteBuffer;

import refit.client.REFITClientLibrary;
import refit.client.REFITClientThread;
import refit.config.REFITConfig;
import refit.message.REFITRequest;
import refit.util.REFITLogger;
import refit.util.REFITTime;
import refit.util.REFITIntervalStatistics.ResultType;


public class REFITLogClient extends REFITClientThread {

	private long lastRequestSeqNr;
	private long gcStart = 0;
	private long ctr = 0;


	public REFITLogClient(REFITClientLibrary service) {
		super(service);
		this.lastRequestSeqNr = -1;
	}


	@Override
	protected boolean work() throws Exception {
		// multiply client id with ratio and check if the part before the point changes
		// this provides a nice approximate client job distribution
		// e.g. ratio = 0.4 -> 0, 0.4, 0.8, 1.2, 1.6, 2.0 -> the third and fifth client are selected
		boolean isGC = service.getNodeID() == REFITConfig.TOTAL_NR_OF_REPLICAS;

		if (isGC) {
			collect();
		} else {
			log();
		}
		return false;
	}

	@Override
	public String toString() {
		return "LOGC-" + (service.getNodeID() - REFITConfig.TOTAL_NR_OF_REPLICAS);
	}

	private void log() throws Exception {
		byte[] data = new byte[REFITConfig.REQUEST_SIZE];
		REFITLogRequest msg = new REFITLogRequest(data);
		msg.serializeMessage();

		long startTime = REFITTime.nanoTime.getAsLong();
		service.invoke(msg.getBuffer(), lastRequestSeqNr, REFITRequest.RequestMode.WRITE_ACK);
		long endTime = REFITTime.nanoTime.getAsLong();
		service.getStatistics().event((long) ((endTime - startTime) / 1000f), service.getNodeID(), "a", ResultType.ACCEPT);

		lastRequestSeqNr = service.getAssignedSeqNr();

		ctr++;

		if (ctr % 10 == 0) {
			// signal collector every ten appends
			service.getSidechannel().add(lastRequestSeqNr);
		}
	}

	private void collect() throws Exception {
		// wait for signal
		long gcEnd = (Long) service.getSidechannel().take();

		REFITLogRequest read = new REFITLogRequest(gcStart, (short) (gcEnd - gcStart));
		read.serializeMessage();

		// Invoke operation
		long startTime = REFITTime.nanoTime.getAsLong();
		ByteBuffer reply = service.invoke(read.getBuffer(), gcEnd - 1, REFITRequest.RequestMode.READ_WEAK);
		long endTime = REFITTime.nanoTime.getAsLong();
		service.getStatistics().event((long) ((endTime - startTime) / 1000f), service.getNodeID(), "r", ResultType.ACCEPT);

		REFITLogReply entry = new REFITLogReply(reply);
		if (entry.data == null)
			throw new Exception("Bad result: Expected data in response, got null");

		REFITLogger.logWarning(this, "Got " + entry.data.length + " entries; base " + gcStart);


		// shorten log
		REFITLogRequest gc = new REFITLogRequest(gcEnd);
		gc.serializeMessage();
		startTime = REFITTime.nanoTime.getAsLong();
		service.invoke(gc.getBuffer(), gcEnd - 1, REFITRequest.RequestMode.WRITE_ACK);
		endTime = REFITTime.nanoTime.getAsLong();
		service.getStatistics().event((long) ((endTime - startTime) / 1000f), service.getNodeID(), "gc", ResultType.ACCEPT);

		gcStart = gcEnd;
	}
}
