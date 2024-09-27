package refit.application;

import java.nio.ByteBuffer;

public class REFITResult {
	private final ByteBuffer reply;

	public REFITResult(ByteBuffer reply) {
		this.reply = reply;
	}

	public ByteBuffer getReply() {
		if (reply != null && reply.remaining() == 0) {
			reply.position(0);
		}
		return reply;
	}
}
