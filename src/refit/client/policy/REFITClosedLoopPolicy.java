package refit.client.policy;

import refit.client.REFITClientTask;
import refit.util.REFITTime;

public class REFITClosedLoopPolicy implements REFITClientPolicy {

	private final long durationInMs;

	public REFITClosedLoopPolicy(long durationInMs, int clientProcessID, int nrOfClients, int clientIDOffset) {
		this.durationInMs = durationInMs;
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
		REFITTime.sleep(durationInMs);
	}
}
