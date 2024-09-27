package refit.client;

import java.nio.ByteBuffer;
import java.util.concurrent.LinkedBlockingQueue;

import refit.application.REFITApplicationServer;
import refit.application.REFITResult;
import refit.application.REFITSystemApplicationServer;
import refit.client.policy.REFITClientPolicy;
import refit.communication.REFITNetworkEndpoint;
import refit.message.REFITEvent;
import refit.message.REFITRequest.RequestMode;
import refit.message.REFITUniqueID;
import refit.scheduler.REFITSchedulerGroup;
import refit.util.REFITIntervalStatistics;

public class REFITLocalClientLibrary extends REFITClientLibrary {
	private final REFITApplicationServer server;
	private final REFITSystemApplicationServer systemServer;
	private long uidCounter = 0;
	private ByteBuffer asyncResult = null;

	public REFITLocalClientLibrary(REFITApplicationServer server, REFITSystemApplicationServer systemServer, REFITSchedulerGroup group,
	                               REFITIntervalStatistics statistics, REFITIntervalStatistics rejectStatistics, REFITClientPolicy policy, boolean isFirstClient,
	                               LinkedBlockingQueue<Object> sidechannel) {
		super((short) 0, group, statistics, rejectStatistics, policy, isFirstClient, sidechannel);
		this.server = server;
		this.systemServer = systemServer;
	}

	@Override
	protected REFITNetworkEndpoint createNetwork(boolean isFirstClient) {
		return null;
	}

	@Override
	public long getProgress() {
		return uidCounter;
	}

	@Override
	public ByteBuffer invoke(ByteBuffer request, long executeAfter, RequestMode mode) {
		if (mode.isReadOnly() && !server.checkReadOnly(request)) {
			throw new IllegalArgumentException();
		}

		REFITApplicationServer app = (mode.equals(RequestMode.SYSTEM)) ? systemServer : server;
		REFITResult result = app.processRequest(new REFITUniqueID(this.getNodeID(), uidCounter++),
				uidCounter, request);
		return result.getReply();
	}

	@Override
	public void invokeAsync(ByteBuffer request, long executeAfter, RequestMode mode) {
		asyncResult = invoke(request, executeAfter, mode);
	}

	@Override
	public boolean isResultStable() {
		return asyncResult != null;
	}

	@Override
	public boolean isReadWriteConflict() {
		return false;
	}

	@Override
	public ByteBuffer getResult() {
		ByteBuffer result = asyncResult;
		asyncResult = null;
		return result;
	}

	@Override
	public long getAssignedSeqNr() {
		return uidCounter;
	}

	@Override
	protected void handleMessage(REFITEvent message) {
		throw new InternalError("This should never be called");
	}
}
