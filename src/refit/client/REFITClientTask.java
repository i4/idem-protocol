package refit.client;

import refit.scheduler.REFITSchedulerTask;
import refit.scheduler.REFITSchedulerTaskType;
import refit.util.REFITLogger;


public abstract class REFITClientTask extends REFITSchedulerTask implements REFITBenchmarkRunnable {

	protected final REFITClientLibrary service;
	private boolean hasPermit;
	private boolean alive;

	public REFITClientTask(REFITClientLibrary service) {
		super(REFITSchedulerTaskType.CLIENT_TASK, service.getGroup());
		this.service = service;
		this.alive = true;
	}

	@Override
	public void init() {
		// enqueue myself for execution
		progress();
	}

	@Override
	// make progress a public function
	public void progress() {
		super.progress();
	}

	protected abstract void issueRequest() throws Exception;

	protected abstract boolean work() throws Exception;

	public void grantPermit() {
		hasPermit = true;
		progress();
	}

	@Override
	public void execute() {
		if (!alive) return;
		try {
			boolean requestComplete = work();
			if (!requestComplete) {
				return;
			}

			if (!hasPermit && service.getPolicy().shouldWaitForMyTurn(service.getNodeID(), this)) {
				return;
			}
			hasPermit = false;
			issueRequest();
		} catch (Exception e) {
			REFITLogger.logWarning(this, "" + e);
			alive = false;
		}
	}
}
