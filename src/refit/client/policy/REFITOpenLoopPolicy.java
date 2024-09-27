package refit.client.policy;

import java.util.Arrays;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.Semaphore;

import refit.client.REFITClientTask;
import refit.config.REFITConfig;
import refit.util.REFITAssert;
import refit.util.REFITTime;

public class REFITOpenLoopPolicy implements REFITClientPolicy {

	private final long durationInMs;
	private final int clientIDOffset;
	private final int clientRate;
	private final Semaphore clientThrottle;
	private final boolean[] firstRun;
	private final int slowStart;
	private final ConcurrentLinkedDeque<REFITClientTask> waitQueue = new ConcurrentLinkedDeque<>();

	public REFITOpenLoopPolicy(long durationInMs, int clientProcessID, int nrOfClients, int clientIDOffset) {
		this.durationInMs = durationInMs;
		this.clientIDOffset = clientIDOffset;

		clientRate = REFITConfig.POLICY_OPEN_LOOP_RATE[clientProcessID];
		REFITAssert.assertTrue(clientRate > 0, "Invalid client rate for open loop policy");
		slowStart = REFITConfig.POLICY_OPEN_LOOP_SLOW_START;

		clientThrottle = new Semaphore(0);
		firstRun = new boolean[nrOfClients];
		Arrays.fill(firstRun, true);
	}

	@Override
	public void waitForMyTurn(short nodeID) throws InterruptedException {
		int offsetID = nodeID - clientIDOffset;
		if (firstRun[offsetID] && slowStart == 0) {
			// don't wait on first run
			firstRun[offsetID] = false;
			return;
		}

		clientThrottle.acquire();
	}

	@Override
	public void execute() throws InterruptedException {
		long expectedEnd = REFITTime.currentTimeMillis.getAsLong() + durationInMs;

		double tokenPerMS = clientRate / 1000.;
		double tokenPerMSSlow = 20 / 1000.;
		// allow at most half second bursts
		int tokenLimit = (int) Math.ceil(tokenPerMS * 500);

		double tokenRemainder = 0;
		long lastTime = REFITTime.currentTimeMillis.getAsLong();
		while (true) {
			long currentTime = REFITTime.currentTimeMillis.getAsLong();
			if (currentTime >= expectedEnd) break;

			double rate = tokenPerMS;
			if (currentTime < expectedEnd - durationInMs + slowStart / 2) {
				// slow-start by limiting the rate of new tokens
				rate = tokenPerMSSlow;
			} else if (currentTime < expectedEnd - durationInMs + slowStart) {
				float rel = (currentTime - (expectedEnd - durationInMs + slowStart / 2)) / (float) (slowStart / 2);
				rate = (1 - rel) * tokenPerMSSlow + rel * tokenPerMS;
			}

			double newTokens = (currentTime - lastTime) * rate + tokenRemainder;
			int newFullTokens = (int) Math.floor(newTokens);

			int availablePermits = clientThrottle.availablePermits();
			int permitsToAdd = Math.min(newFullTokens, tokenLimit - availablePermits);

			permitsToAdd = wakeTasks(permitsToAdd);
			clientThrottle.release(permitsToAdd);

			lastTime = currentTime;
			tokenRemainder = newTokens - newFullTokens;

			REFITTime.sleep(10);
		}
	}

	private int wakeTasks(int permitsToAdd) {
		while (!waitQueue.isEmpty() && permitsToAdd > 0) {
			REFITClientTask task = waitQueue.removeFirst();
			task.grantPermit();
			permitsToAdd--;
		}
		while (!waitQueue.isEmpty()) {
			if (clientThrottle.tryAcquire()) {
				REFITClientTask task = waitQueue.removeFirst();
				task.grantPermit();
			} else {
				break;
			}
		}
		return permitsToAdd;
	}

	@Override
	public boolean shouldWaitForMyTurn(short nodeID, REFITClientTask task) {
		if (clientThrottle.tryAcquire()) {
			return false;
		}
		waitQueue.add(task);
		return true;
	}
}
