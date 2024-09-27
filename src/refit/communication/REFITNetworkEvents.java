package refit.communication;

import refit.message.REFITEventClass;

public class REFITNetworkEvents {

	public static final class REFITInitialConnectionsCompleted extends REFITEventClass {
		public final short nodeID;

		public REFITInitialConnectionsCompleted(short nodeID) {
			this.nodeID = nodeID;
		}

		@Override
		public String toString() {
			return "{REFITInitialConnectionsCompleted|" + nodeID + "}";
		}
	}

	public static final class REFITConnectionResetNotification extends REFITEventClass {
		public final short nodeID;

		public REFITConnectionResetNotification(short nodeID) {
			this.nodeID = nodeID;
		}

		@Override
		public String toString() {
			return "{REFITConnectionResetNotification|" + nodeID + "}";
		}
	}

	public static final class REFITConnectionWriteableNotification extends REFITEventClass {
		public final short nodeID;

		public REFITConnectionWriteableNotification(short nodeID) {
			this.nodeID = nodeID;
		}

		@Override
		public String toString() {
			return "{REFITConnectionWriteableNotification|" + nodeID + "}";
		}
	}
}
