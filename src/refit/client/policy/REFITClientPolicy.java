package refit.client.policy;

import refit.client.REFITClientTask;

public interface REFITClientPolicy {

	// must provide a constructor compatible with REFITClientPolicy(long durationInMs, int clientProcessID, int nrOfClients, int clientIDOffset)

	// Wait for the turn of this node
	void waitForMyTurn(short nodeID) throws InterruptedException;

	// Tell task to wait for its turn, must signal the task when it is time
	boolean shouldWaitForMyTurn(short nodeID, REFITClientTask task);

	// Run the benchmark for `durationInMs` and ensure let clients proceed according to the expected policy
	void execute() throws InterruptedException;
}
