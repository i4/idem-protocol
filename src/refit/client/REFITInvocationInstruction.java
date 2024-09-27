package refit.client;

import java.nio.ByteBuffer;

import refit.message.REFITEventClass;
import refit.message.REFITRequest.RequestMode;

public final class REFITInvocationInstruction extends REFITEventClass {
	public final ByteBuffer request;
	public final long executeAfter;
	public final RequestMode mode;

	public REFITInvocationInstruction() {
		this(null, -1, null);
	}

	public REFITInvocationInstruction(ByteBuffer request, long executeAfter, RequestMode mode) {
		this.request = request;
		this.executeAfter = executeAfter;
		this.mode = mode;
	}
}
