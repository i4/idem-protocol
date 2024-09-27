package refit.client.policy;

import refit.application.ycsb.REFITYCSBBenchmark;
import refit.client.REFITClientTask;
import refit.config.REFITConfig;
import refit.util.REFITTime;

public class REFITYCSBPolicy implements REFITClientPolicy {

	private final long durationInMs;
	private final int clientProcessID;

	public REFITYCSBPolicy(long durationInMs, int clientProcessID, int nrOfClients, int clientIDOffset) {
		this.durationInMs = durationInMs;
		this.clientProcessID = clientProcessID;
	}

	@Override
	public void waitForMyTurn(short nodeID) {
		// no-op
	}

	@Override
	public boolean shouldWaitForMyTurn(short nodeID, REFITClientTask task) {
		// no-op
		return false;
	}

	@Override
	public void execute() throws InterruptedException {
		executeInner();
		if (REFITConfig.NR_OF_CLIENTS.length > 1 && clientProcessID == 0) {
			// give other clients a chance to finish
			Thread.sleep(100 * 1000);
		}
	}

	private void executeInner() throws InterruptedException {
		long endTime = REFITTime.currentTimeMillis.getAsLong() + durationInMs;
		while (true) {
			long current = REFITTime.currentTimeMillis.getAsLong();
			if (current >= endTime) return;
			if (REFITYCSBBenchmark.isFinished) return;

			REFITTime.sleep(Math.min(endTime - current, 1000));
		}
	}
}
