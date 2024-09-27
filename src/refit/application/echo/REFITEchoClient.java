package refit.application.echo;

import java.nio.ByteBuffer;

import refit.client.REFITClientLibrary;
import refit.client.REFITClientThread;
import refit.config.REFITConfig;
import refit.message.REFITRequest.RequestMode;
import refit.util.REFITTime;
import refit.util.REFITIntervalStatistics.ResultType;


public class REFITEchoClient extends REFITClientThread {
	private final ByteBuffer request;


	public REFITEchoClient(REFITClientLibrary service) {
		super(service);
		request = ByteBuffer.allocate(REFITConfig.REQUEST_SIZE);
	}

	@Override
	protected boolean work() throws Exception {
		// Invoke operation
		long startTime = REFITTime.nanoTime.getAsLong();
		ByteBuffer result = service.invoke(request, RequestMode.WRITE);
		long endTime = REFITTime.nanoTime.getAsLong();
		service.getStatistics().event((long) ((endTime - startTime) / 1000f), service.getNodeID(), "e", ResultType.ACCEPT);

		// Check result
		if (!request.equals(result)) throw new Exception("Bad result: unexpected echo");
		return false;
	}
}
