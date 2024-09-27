package refit.application.log;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Set;

import refit.application.REFITApplicationServer;
import refit.application.REFITResult;
import refit.message.REFITUniqueID;
import refit.replica.checkpoint.REFITCheckpointObject;

public class REFITLogServer implements REFITApplicationServer {

	private static final ByteBuffer ZERO_BB = ByteBuffer.allocate(0);

	private ArrayList<LogEntry> queue;
	private int startOffset;
	private long gcOffset;
	private long lastSeqNr;
	private short subSeqNr;

	public REFITLogServer() {
		this.queue = new ArrayList<>();
		this.startOffset = 0;
		this.gcOffset = 0;
	}


	@Override
	public void init() {
		// Do nothing
	}

	@Override
	public boolean checkReadOnly(ByteBuffer payload) {
		REFITLogRequest msg = new REFITLogRequest(payload);
		return msg.operation == REFITLogRequest.LogOperation.READ_RANGE;
	}

	@Override
	public REFITResult processRequest(REFITUniqueID uid, long agreementSeqNr, ByteBuffer payload) {
		REFITLogRequest msg = new REFITLogRequest(payload);

		if (msg.operation != REFITLogRequest.LogOperation.READ_RANGE) {
			if (agreementSeqNr != lastSeqNr) {
				lastSeqNr = agreementSeqNr;
				subSeqNr = 0;
			} else {
				subSeqNr++;
			}
		}

		REFITLogReply reply;
		switch (msg.operation) {
		case APPEND:
			queue.add(new LogEntry(lastSeqNr, subSeqNr, msg.data));
			reply = new REFITLogReply();
			break;
		case READ_RANGE: {
			if (msg.start < gcOffset) {
				reply = new REFITLogReply();
				break;
			}

			int idx = Collections.binarySearch(queue, new LogEntry(msg.start, (short) 0, null), Comparator.comparingLong((LogEntry a) -> a.seqNr).thenComparingInt(a -> a.subSeq));
			if (idx < 0) {
				idx = -idx - 1;
			}

//			if (queue.size() > 0) {
//				REFITLogger.logWarning(this, "At start " + queue.get(0).seqNr + " idx " + idx + "@" +queue.get(idx).seqNr + " msg " + msg.start);
//			} else {
//				REFITLogger.logWarning(this, "Empty queue");
//			}

			ArrayList<byte[]> data = new ArrayList<>();
			while (idx < queue.size() && queue.get(idx).seqNr < msg.start + msg.count) {
				data.add(queue.get(idx).data);
				idx++;
			}
//			REFITLogger.logWarning(this, "data len " + data.size());
//			if (idx >= queue.size()) {
//				REFITLogger.logWarning(this, "End oob " + idx);
//			} else {
//				REFITLogger.logWarning(this, "End " + queue.get(idx).seqNr + " eq " + queue.get(queue.size()-1).seqNr);
//			}


			reply = new REFITLogReply(data.toArray(new byte[0][]), idx >= queue.size());
			break;
		}
		case GARBAGE_COLLECT:
			gcOffset = Math.max(gcOffset, msg.start);

			int idx = Collections.binarySearch(queue, new LogEntry(msg.start, (short) 0, null), Comparator.comparingLong((LogEntry a) -> a.seqNr).thenComparingInt(a -> a.subSeq));
			if (idx < 0) {
				idx = -idx - 1;
			}

			startOffset = Math.max(startOffset, idx);

			if (startOffset > 1000) {
				// don't copy every time
				ArrayList<LogEntry> newList = new ArrayList<>();
				for (int i = startOffset; i < queue.size(); i++) {
					newList.add(queue.get(i));
				}
				queue = newList;
				startOffset = 0;
			}

			reply = new REFITLogReply();
			break;
		default:
			throw new IllegalArgumentException("Whoops");
		}

		reply.serializeMessage();

		// Return result
		return new REFITResult(reply.getBuffer());
	}

	@Override
	public String toString() {
		return "LOG";
	}

	@Override
	public REFITCheckpointObject[] createCheckpoint() {
		int len = Long.BYTES + Long.BYTES + Short.BYTES + Integer.BYTES;
		for (int i = startOffset; i < queue.size(); i++) {
			len += Long.BYTES + Short.BYTES + Short.BYTES + queue.get(i).data.length;
		}

		ByteBuffer checkpoint = ByteBuffer.allocate(len);
		checkpoint.putLong(gcOffset);
		checkpoint.putLong(lastSeqNr);
		checkpoint.putShort(subSeqNr);
		checkpoint.putInt(queue.size() - startOffset);
		for (int i = startOffset; i < queue.size(); i++) {
			LogEntry entry = queue.get(i);
			checkpoint.putLong(entry.seqNr);
			checkpoint.putShort(entry.subSeq);
			checkpoint.putShort((short) entry.data.length);
			checkpoint.put(entry.data);
		}
		return new REFITCheckpointObject[]{new REFITCheckpointObject(checkpoint.array())};
	}

	@Override
	public void applyCheckpoint(final REFITCheckpointObject[] checkpoint) {
//		REFITLogger.logDebug(this, "Apply checkpoint");
		ByteBuffer checkpointBuffer = checkpoint[0].getBuffer();
		startOffset = 0;
		gcOffset = checkpointBuffer.getLong();
		lastSeqNr = checkpointBuffer.getLong();
		subSeqNr = checkpointBuffer.getShort();
		int queueLen = checkpointBuffer.getInt();
		queue.clear();
		for (int i = 0; i < queueLen; i++) {
			long seqNr = checkpointBuffer.getLong();
			short subSeq = checkpointBuffer.getShort();
			byte[] bytes = new byte[checkpointBuffer.getShort()];
			checkpointBuffer.get(bytes);
			queue.add(new LogEntry(seqNr, subSeq, bytes));
		}
	}

	private static class LogEntry {
		final long seqNr;
		final short subSeq;
		final byte[] data;

		public LogEntry(long seqNr, short subSeq, byte[] data) {
			this.seqNr = seqNr;
			this.subSeq = subSeq;
			this.data = data;
		}
	}
}
