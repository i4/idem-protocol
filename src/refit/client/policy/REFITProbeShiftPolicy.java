package refit.client.policy;

import refit.client.REFITClientTask;
import refit.config.REFITConfig;
import refit.util.REFITLogger;
import refit.util.REFITTime;

public class REFITProbeShiftPolicy implements REFITClientPolicy {
	private final long durationInMs;
	private final long timeSegment;
	private final long rampUp;
	private final long activeDuration;

	private final int clientIDOffset;
	private final int nrOfClients;

	private final Object[] clientSyncs;
	private final boolean[] clientActiveFlag;


	public REFITProbeShiftPolicy(long durationInMs, int clientProcessID, int nrOfClients, int clientIDOffset) {
		// overlapping clients: 1/6 ramp up + 4/6 active + 1/6 ramp down
		// locations overlap on ramp up and ramp down
		this.durationInMs = durationInMs;

		// group 4 is faulty
		// multiply with 5 as ramp up and down overlap; +1 as first and last ramp don't overlap
		int count = Math.min(REFITConfig.NR_OF_CLIENTS.length, 4);
		int timeParts = count * 5 + 1;
		timeSegment = durationInMs / timeParts;

		if (clientProcessID < 4) {
			rampUp = clientProcessID * 5 * timeSegment;
			activeDuration = 4 * timeSegment;
		} else {
			rampUp = 0;
			activeDuration = (timeParts - 2) * timeSegment;
		}

		this.clientIDOffset = clientIDOffset;
		this.nrOfClients = nrOfClients;
		clientSyncs = new Object[nrOfClients];
		for (int i = 0; i < clientSyncs.length; i++) {
			clientSyncs[i] = new Object();
		}
		clientActiveFlag = new boolean[nrOfClients];

		REFITLogger.logDebug(this, "ShiftPolicy: Timestep " + timeSegment + "ms, rampUp " + rampUp
				+ "ms, activePhase " + activeDuration + "ms");
	}

	@Override
	public void waitForMyTurn(short nodeID) throws InterruptedException {
		int idx = nodeID - clientIDOffset;
		synchronized (clientSyncs[idx]) {
			while (!clientActiveFlag[idx]) {
				clientSyncs[idx].wait();
			}
		}
	}

	@Override
	public void execute() throws InterruptedException {
		long start = REFITTime.currentTimeMillis.getAsLong();
		if (rampUp > 0) {
			REFITTime.sleep(rampUp);
		}

		// ramp up
		clientRamp(true);

		// active phase
		REFITTime.sleep(start + rampUp + timeSegment + activeDuration - REFITTime.currentTimeMillis.getAsLong());

		// ramp down
		clientRamp(false);

		// wait for benchmark end
		long remaining = durationInMs - (REFITTime.currentTimeMillis.getAsLong() - start);
		if (remaining > 0) {
			REFITTime.sleep(remaining);
		}
	}

	private void clientRamp(boolean enable) throws InterruptedException {
		long rampStart = REFITTime.currentTimeMillis.getAsLong();
		for (int i = 0; i < nrOfClients; i++) {
			// client pair of normal and probe client
			synchronized (clientSyncs[i]) {
				clientActiveFlag[i] = enable;
				clientSyncs[i].notifyAll();
			}

			if ((i % 2) == 0) {
				long until = rampStart + (i / 2 + 1) * timeSegment / (nrOfClients / 2);
				long left = until - REFITTime.currentTimeMillis.getAsLong();
				if (left > 0) {
					REFITTime.sleep(left);
				}
			}
		}
	}

	@Override
	public boolean shouldWaitForMyTurn(short nodeID, REFITClientTask task) {
		throw new UnsupportedOperationException("client task are not supported by this policy");
	}
}
