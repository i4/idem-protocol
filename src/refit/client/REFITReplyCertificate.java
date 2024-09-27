package refit.client;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

import refit.config.REFITConfig;
import refit.crypto.REFITMessageAuthentication;
import refit.message.REFITMessageType;
import refit.message.REFITReplyBase;
import refit.message.REFITUniqueID;
import refit.util.REFITBallotBox;
import refit.util.REFITIntervalStatistics;
import refit.util.REFITLogger;


public class REFITReplyCertificate {

	public final REFITBallotBox<Short, ByteBuffer, REFITReplyBase> replies;
	private static final int STABILITY_THRESHOLD = (REFITConfig.AUTHENTICATE_MESSAGES) ? REFITConfig.FAULTS_TO_TOLERATE + 1 : 1;
	private static final int READOPT_STABILITY_THRESHOLD = (REFITConfig.AUTHENTICATE_MESSAGES)
			? (REFITConfig.TOTAL_NR_OF_REPLICAS + REFITConfig.FAULTS_TO_TOLERATE + 1 + 1) / 2
			: (REFITConfig.TOTAL_NR_OF_REPLICAS + 1 + 1) / 2;

	private final REFITMessageAuthentication messageAuthentication;
	private REFITUniqueID uid;
	public REFITReplyBase result;
	private final boolean[] legitimateSenders;
	private long requestTime;
	private short[] replyTimes = new short[REFITConfig.TOTAL_NR_OF_REPLICAS];
	private boolean isTotalOrder;
	private boolean isAck;

	private final REFITIntervalStatistics statistics;

	public REFITReplyCertificate(REFITMessageAuthentication messageAuthentication, REFITIntervalStatistics statistics, boolean[] legitimateSenders) {
		this.replies = new REFITBallotBox<>(REFITConfig.USE_PBFT_READ_OPTIMIZATION ? READOPT_STABILITY_THRESHOLD : STABILITY_THRESHOLD);
		this.messageAuthentication = messageAuthentication;
		this.statistics = statistics;
		this.legitimateSenders = legitimateSenders;
	}


	public void init(REFITUniqueID uid, boolean isTotalOrder, boolean isAck) {
		replies.clear();
		this.uid = uid;
		result = null;
		requestTime = System.currentTimeMillis();
		Arrays.fill(replyTimes, (short) Math.min(Short.MAX_VALUE, REFITConfig.CLIENT_REQUEST_TIMEOUT));
		this.isTotalOrder = isTotalOrder;
		this.isAck = isAck;
	}

	public boolean add(REFITReplyBase reply) {
		long replyTime = System.currentTimeMillis();
		// Check reply
		if (reply == null) return false;
		if (!uid.equals(reply.uid)) return false;
		if (isAck && reply.type != REFITMessageType.ACK_REPLY || !isAck && reply.type != REFITMessageType.REPLY)
			return false;
		if (!messageAuthentication.verifyUnicastMAC(reply, legitimateSenders)) return false;

		// Prepare vote
		ByteBuffer vote = ByteBuffer.wrap(reply.getPayloadHash());

		// Check whether the reply is a late full reply
		ByteBuffer decision = replies.getDecision();
		if (reply.isFullReply() && vote.equals(decision)) {
			result = reply;
			return true;
		}

		// Cast vote
		boolean success = replies.add(reply.from, vote, reply);
		if (success) {
			replyTimes[reply.from] = (short) (replyTime - requestTime);
			checkResult();
		}
		return success;
	}

	public boolean isStable() {
		return (result != null);
	}

	public short[] getReplyTimes() {
		return replyTimes;
	}

	public int[] getPerExecutorViewId() {
		int[] perExecutorViewId = new int[REFITConfig.TOTAL_NR_OF_REPLICAS];
		for (REFITReplyBase reply : replies.getDecidingBallots()) {
			perExecutorViewId[reply.from] = reply.viewID();
		}
		return perExecutorViewId;
	}

	private void checkResult() {
		if (isStable()) return;
		if (REFITConfig.ENABLE_DEBUG_CHECKS && isTotalOrder && replies.votesDiffer())
			REFITLogger.logWarning("[REPLY]", "received different results for request " + uid + " " + replies);
		List<REFITReplyBase> correctReplies = replies.getDecidingBallots();
		if (correctReplies == null) return;
		for (REFITReplyBase reply : correctReplies) {
			if (!reply.isFullReply()) continue;
			result = reply;
			break;
		}
		if (REFITConfig.CLIENT_REPLY_STATISTICS) updateStats(correctReplies);
	}

	public boolean isCompletable() {
		if (isStable()) {
			return true;
		}
		int remainingVotes = REFITConfig.TOTAL_NR_OF_REPLICAS - replies.getVoteCount();
		int largestBallot = replies.getLargestBallotSize();

		return remainingVotes + largestBallot >= replies.getDecisionThreshold();
	}

	private void updateStats(List<REFITReplyBase> correctReplies) {
		for (REFITReplyBase reply : correctReplies) {
			statistics.trackReply(reply.from);
		}
	}

}
