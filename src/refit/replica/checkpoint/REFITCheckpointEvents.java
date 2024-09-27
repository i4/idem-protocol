package refit.replica.checkpoint;

import refit.message.REFITEventClass;

public class REFITCheckpointEvents {
	public static final class REFITCPProgressNotification extends REFITEventClass {
		public final long checkpointCtr;
		public final long execCtr;
		public final long agreementProgress;

		public REFITCPProgressNotification(long checkpointCtr, long execCtr, long agreementProgress) {
			this.checkpointCtr = checkpointCtr;
			this.execCtr = execCtr;
			this.agreementProgress = agreementProgress;
		}
	}
}
