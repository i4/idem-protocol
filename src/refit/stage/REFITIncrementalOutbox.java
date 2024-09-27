package refit.stage;

import java.util.ArrayDeque;
import java.util.Deque;

import refit.config.REFITConfig;
import refit.message.REFITMessage;

public class REFITIncrementalOutbox implements REFITOutbox {
	private final REFITBoundedReplicaContext replica;
	private final REFITBoundedStage stage;
	private final IndividualOutbox[] outboxes;
	private final int outboxSizeLimit;
	private long globalSendLimit = -1;
	private final boolean strictMonotonic;
	private final boolean[] writeBlocked;

	public REFITIncrementalOutbox(REFITBoundedReplicaContext replica, REFITBoundedStage stage, int outboxSizeLimit) {
		this(replica, stage, outboxSizeLimit, false);
	}

	public REFITIncrementalOutbox(REFITBoundedReplicaContext replica, REFITBoundedStage stage, int outboxSizeLimit, boolean strictMonotonic) {
		this.replica = replica;
		this.stage = stage;
		this.outboxSizeLimit = outboxSizeLimit;
		this.strictMonotonic = strictMonotonic;

		// lazy initialize individual outboxes
		this.outboxes = new IndividualOutbox[REFITConfig.TOTAL_NR_OF_REPLICAS];

		writeBlocked = new boolean[REFITConfig.TOTAL_NR_OF_REPLICAS];
		stage.registerOutbox(this);
	}

	@Override
	public REFITBoundedStage getStage() {
		return stage;
	}

	public void queue(REFITMessage message, boolean[] recipients) {
		for (int i = 0; i < recipients.length; i++) {
			if (!recipients[i]) {
				continue;
			}

			ensureOutbox(i);
			outboxes[i].queue(message);
		}
		stage.enqueueOutbox(this);
	}

	private void ensureOutbox(int idx) {
		if (outboxes[idx] == null) {
			outboxes[idx] = new IndividualOutbox((short) idx, globalSendLimit);
		}
	}

	@Override
	public void requeue(short replicaID) {
		if (outboxes[replicaID] != null) {
			outboxes[replicaID].resend();
			stage.enqueueOutbox(this);
		}
	}

	public void collectGarbageBefore(long seqNr) {
		for (IndividualOutbox outbox : outboxes) {
			if (outbox == null) {
				continue;
			}

			outbox.collectGarbageBefore(seqNr);
		}
		stage.enqueueOutbox(this);
	}

	public void collectIndividualGarbageBefore(short replicaID, long seqNr) {
		ensureOutbox(replicaID);
		outboxes[replicaID].collectGarbageBefore(seqNr);
		stage.enqueueOutbox(this);
	}

	@SuppressWarnings("BooleanMethodIsAlwaysInverted")
	public boolean isAfterGarbageLimit(long seqNr) {
		boolean noOutboxFound = true;
		for (IndividualOutbox outbox : outboxes) {
			if (outbox == null) {
				continue;
			}
			noOutboxFound = false;
			if (seqNr >= outbox.getGarbageBefore()) {
				return true;
			}
		}
		return noOutboxFound;
	}

	// limitSeqNr is exclusive
	public void updateSendLimits(long limitSeqNr) {
		if (limitSeqNr < globalSendLimit) {
			throw new IllegalArgumentException("Send limit must only increase");
		}
		globalSendLimit = limitSeqNr;
		for (IndividualOutbox outbox : outboxes) {
			if (outbox == null) {
				continue;
			}

			outbox.updateSendLimit(limitSeqNr);
		}
		stage.enqueueOutbox(this);
	}

	// limitSeqNr is exclusive
	public void updateIndividualSendLimit(short replicaID, long limitSeqNr) {
		if (limitSeqNr < globalSendLimit) {
			throw new IllegalArgumentException("Send limit must only increase");
		}
		ensureOutbox(replicaID);
		outboxes[replicaID].updateSendLimit(limitSeqNr);
		stage.enqueueOutbox(this);
	}

	@Override
	public void trySend() {
		for (IndividualOutbox outbox : outboxes) {
			if (outbox == null) {
				continue;
			}

			outbox.trySend();
		}
	}

	@Override
	public void notifyWriteable(short replicaID) {
		writeBlocked[replicaID] = false;
	}


	private class IndividualOutbox {
		private final short replicaID;
		private final Deque<REFITMessage> sent;
		private final Deque<REFITMessage> outbox;

		private long garbageBefore = -1;
		private long lastSeqNr = -1;
		// excluding the seqNr
		private long sendLimitSeqNr;

		public IndividualOutbox(short replicaID, long sendLimit) {
			this.replicaID = replicaID;
			this.sendLimitSeqNr = sendLimit;

			sent = new ArrayDeque<>();
			outbox = new ArrayDeque<>();
		}

		public void queue(REFITMessage message) {
			if (queueCapacity() <= 0) {
				throw new IllegalStateException("Message would violate outbox size limit");
			}
			if (message.uid.seqNr < lastSeqNr || (strictMonotonic && message.uid.seqNr == lastSeqNr)) {
				throw new IllegalStateException("Messages must have monotonically increasing sequence numbers!");
			}
			lastSeqNr = message.uid.seqNr;
			// drop very old messages
			if (message.uid.seqNr >= garbageBefore) {
				outbox.add(message);
			}
		}

		public int queueCapacity() {
			return outboxSizeLimit - outbox.size() - sent.size();
		}

		private boolean hasWaitingRequest() {
			return !outbox.isEmpty() && outbox.peekFirst().uid.seqNr < sendLimitSeqNr;
		}

		public void trySend() {
			if (writeBlocked[replicaID]) return;

			while (hasWaitingRequest()) {
				boolean wasAccepted = replica.replicaBoundedUnicast(outbox.peek(), replicaID, REFITIncrementalOutbox.this);
				if (wasAccepted) {
					// size of sent queue is already checked in queue method
					REFITMessage message = outbox.poll();
					sent.add(message);
				} else {
					break;
				}
			}
		}

		public void resend() {
			// move sent messages back to outbox
			while (!sent.isEmpty()) {
				outbox.addFirst(sent.removeLast());
			}
		}

		public void updateSendLimit(long limitSeqNr) {
			if (limitSeqNr < sendLimitSeqNr) {
				throw new IllegalArgumentException("Send limit must only increase");
			}
			sendLimitSeqNr = limitSeqNr;
		}

		public void collectGarbageBefore(long seqNr) {
			if (seqNr < garbageBefore) {
				return;
			}
			garbageBefore = seqNr;
			garbageCollectQueue(seqNr, sent);
			garbageCollectQueue(seqNr, outbox);
		}

		private void garbageCollectQueue(long seqNr, Deque<REFITMessage> queue) {
			while (!queue.isEmpty()) {
				if (queue.peek().uid.seqNr < seqNr) {
					queue.removeFirst();
				} else {
					break;
				}
			}
		}

		public long getGarbageBefore() {
			return garbageBefore;
		}
	}
}
