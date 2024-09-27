package refit.stage;

public interface REFITOutbox {

	REFITBoundedStage getStage();

	void trySend();

	void requeue(short nodeID);

	void notifyWriteable(short nodeId);
}
