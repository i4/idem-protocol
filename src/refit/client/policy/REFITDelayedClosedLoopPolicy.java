package refit.client.policy;

import java.util.concurrent.atomic.AtomicIntegerArray;

import refit.client.REFITClientTask;
import refit.util.REFITTime;

public class REFITDelayedClosedLoopPolicy implements REFITClientPolicy {

	private final long durationInMs;
	private final int clientIDOffset;

	private final AtomicIntegerArray sleepMarkers;
	private final int startDelay;

	public REFITDelayedClosedLoopPolicy(long durationInMs, int clientProcessID, int nrOfClients, int clientIDOffset) {
		this.durationInMs = durationInMs;
		this.clientIDOffset = clientIDOffset;
		sleepMarkers = new AtomicIntegerArray(nrOfClients);
		startDelay = clientProcessID * 30;
	}

	@Override
	public void waitForMyTurn(short nodeID) throws InterruptedException {
		int offsetID = nodeID - clientIDOffset;
		while (sleepMarkers.get(offsetID) > 0) {
			REFITTime.sleep(100);
		}
	}

	@Override
	public void execute() throws InterruptedException {
		REFITTime.sleep(5000);
		for (int i = 0; i < sleepMarkers.length(); i++) {
			sleepMarkers.set(i, 1);
		}

		REFITTime.sleep(startDelay * 1000);

		for (int i = 0; i < sleepMarkers.length(); i++) {
			sleepMarkers.set(i, 0);
			REFITTime.sleep(1000);
		}

		REFITTime.sleep(durationInMs - 2 * sleepMarkers.length() * 1000);

		for (int i = 0; i < sleepMarkers.length(); i++) {
			sleepMarkers.set(i, 1);
			REFITTime.sleep(1000);
		}
	}

	@Override
	public boolean shouldWaitForMyTurn(short nodeID, REFITClientTask task) {
		throw new UnsupportedOperationException("client task are not supported by this policy");
	}
}
