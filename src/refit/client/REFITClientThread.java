package refit.client;

import refit.config.REFITConfig;
import refit.util.REFITLogger;

public abstract class REFITClientThread implements Runnable, REFITBenchmarkRunnable {

	protected final REFITClientLibrary service;


	public REFITClientThread(REFITClientLibrary service) {
		this.service = service;
	}

	protected abstract boolean work() throws Exception;

	@Override
	public void run() {
		while (!Thread.currentThread().isInterrupted()) {
			try {
				service.getPolicy().waitForMyTurn(service.getNodeID());
				boolean reject = work();
				if (reject) {
				// After reject, wait a semi random timeout before trying next request
				try {
					int timeout =  (int) ((Math.random() + 1) * (REFITConfig.REJECT_MAX_IDLE/2));
					REFITLogger.logClient(this, service.getNodeID() + " waiting for " + timeout + "ms after reject.");
					Thread.sleep(timeout);
				} catch (InterruptedException e) {}
			}
			} catch (Exception e) {
				e.printStackTrace();
				break;
			}
		}
	}

}
