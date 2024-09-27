package refit.communication;

import java.nio.ByteBuffer;

public interface REFITNetworkInboundRouter extends REFITScheduledRouter {
	void receiveMessage(ByteBuffer message, short nodeID);
}
