package refit.application;

import refit.client.REFITClientLibrary;
import refit.client.REFITClientThread;


public class REFITNopClient extends REFITClientThread {
	public REFITNopClient(REFITClientLibrary service) {
		super(service);
	}

	@Override
	protected boolean work() throws Exception {
		Thread.sleep(1000);
		return false;
	}
}
