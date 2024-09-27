package refit.replica.checkpoint;

import refit.message.REFITEventClass;

public final class REFITCheckpointAssist extends REFITEventClass {
	public final int groupIdx;
	public final long minCheckpointSeqNr;
	public final short fullSender;

	public REFITCheckpointAssist(int groupIdx, long minCheckpointSeqNr, short fullSender) {
		this.groupIdx = groupIdx;
		this.minCheckpointSeqNr = minCheckpointSeqNr;
		this.fullSender = fullSender;
	}
}
