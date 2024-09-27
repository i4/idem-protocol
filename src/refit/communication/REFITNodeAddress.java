package refit.communication;

import java.net.SocketAddress;

public class REFITNodeAddress {
	public final short nodeID;
	public final SocketAddress address;

	public REFITNodeAddress(short nodeID, SocketAddress address) {
		this.nodeID = nodeID;
		this.address = address;
	}

	@Override
	public String toString() {
		return nodeID + "@" + address;
	}
}
