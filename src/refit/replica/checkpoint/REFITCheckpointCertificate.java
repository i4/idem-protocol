package refit.replica.checkpoint;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

import refit.config.REFITConfig;
import refit.crypto.REFITMessageAuthentication;
import refit.util.REFITBallotBox;
import refit.util.REFITLogger;
import refit.util.REFITPayload;


public class REFITCheckpointCertificate {

	public final long checkpointCtr;
	private boolean[] legitimateSenders;
	private final REFITBallotBox<Short, REFITCheckpointCertificateVote, REFITCheckpoint> checkpoints;
	private final HashSet<Short> obsolete;

	public enum AddResult {
		INVALID,
		DUPLICATE,
		ADDED
	}

	public REFITCheckpointCertificate(long checkpointCtr, boolean[] legitimateSenders) {
		this.checkpointCtr = checkpointCtr;
		this.legitimateSenders = legitimateSenders;
		this.checkpoints = new REFITBallotBox<>(REFITConfig.REGULAR_CHECKPOINT_STABILITY_THRESHOLD);
		this.obsolete = new HashSet<>();
	}

	public AddResult add(REFITCheckpoint checkpoint, REFITMessageAuthentication mac) {
		if (checkpoint == null) return AddResult.INVALID;
		if (checkpointCtr != checkpoint.uid.seqNr) return AddResult.INVALID;
		if (!mac.verifySignature(checkpoint, legitimateSenders)) return AddResult.INVALID;
		REFITCheckpointCertificateVote vote = new REFITCheckpointCertificateVote(checkpoint.contentHash, checkpoint.execCtr, checkpoint.agreementProgress);
		boolean wasAdded = checkpoints.add(checkpoint.from, vote, checkpoint);
		return wasAdded ? AddResult.ADDED : AddResult.DUPLICATE;
	}

	public boolean isStable() {
		REFITCheckpointCertificateVote decision = checkpoints.getDecision();
		return (decision != null);
	}

	public List<REFITCheckpoint> getDecidingBallots() {
		return checkpoints.getDecidingBallots();
	}

	public REFITCheckpoint getStableCheckpoint() {
		// Check whether the checkpoint has become stable
		List<REFITCheckpoint> correctCheckpoints = getDecidingBallots();
		if (correctCheckpoints == null) return null;

		// Return checkpoint with state if available
		for (REFITCheckpoint correctCheckpoint : correctCheckpoints) {
			if (correctCheckpoint.hasState()) {
				return correctCheckpoint;
			}
		}

		// No full checkpoint available -> Return a correct hash checkpoint
		return correctCheckpoints.get(0);
	}

	public void markSenderObsolete(short sender) {
		if (checkpoints.hasVoted(sender)) {
			obsolete.add(sender);
		}
	}

	public boolean isObsolete() {
		return obsolete.size() == checkpoints.getVoteCount();
	}


	// ######################
	// # CHECKPOINT CONTENT #
	// ######################

	private static class REFITCheckpointCertificateVote {

		private final byte[] hash;
		private final long execCtr;
		private final long agreementProgress;


		public REFITCheckpointCertificateVote(byte[] hash, long execCtr, long agreementProgress) {
			this.hash = hash;
			this.execCtr = execCtr;
			this.agreementProgress = agreementProgress;
		}

		@Override
		public boolean equals(Object object) {
			if (this == object) return true;
			if (object == null || getClass() != object.getClass()) return false;
			REFITCheckpointCertificateVote other = (REFITCheckpointCertificateVote) object;
			if (execCtr != other.execCtr) {
				REFITLogger.logWarning("[CHKPT]", "checkpoint execution counter mismatch: "
						+ execCtr + " vs " + other.execCtr);
			}
			if (agreementProgress != other.agreementProgress) {
				REFITLogger.logWarning("[CHKPT]", "checkpoint progress mismatch: "
						+ agreementProgress + " vs " + other.agreementProgress);
			}
			if (!Arrays.equals(hash, other.hash)) {
				REFITLogger.logWarning("[CHKPT]", "checkpoint content mismatch: "
						+ REFITPayload.toString(hash) + " vs " + REFITPayload.toString(other.hash));
				return false;
			}
			return true;
		}

		@Override
		public int hashCode() {
			// fixed hash code to ensure that the checkpoint content comparison works
			// the comparison will only ever happen between a few objects so this doesn't get too inefficient
			return 0;
		}
	}

}
