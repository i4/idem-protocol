package refit.util;

import java.util.*;
import java.util.Map.Entry;


public class REFITBallotBox<ID extends Short, Vote, Ballot> {

	private final int decisionThreshold;
	private final Set<ID> ids;
	private final Map<Vote, List<Ballot>> votes;
	private final Map<Vote, Integer> voteWeights;
	private final int[] weights;


	public REFITBallotBox(int decisionThreshold) {
		this(decisionThreshold, null);
	}

	public REFITBallotBox(int decisionThreshold, int[] weights) {
		this.decisionThreshold = decisionThreshold;
		this.ids = new HashSet<>();
		this.votes = new HashMap<>();
		this.voteWeights = new HashMap<>();
		this.weights = weights;
	}


	@Override
	public String toString() {
		return "{" + ids + ", " + votes + "}";
	}


	public void clear() {
		ids.clear();
		votes.clear();
		voteWeights.clear();
	}

	public int getDecisionThreshold() {
		return decisionThreshold;
	}

	public int getVoteCount() {
		return ids.size();
	}

	public String getVoteList() {
		return ids.toString();
	}

	public int getLargestBallotSize() {
		int size = 0;
		for (List<Ballot> ballot : votes.values()) {
			size = Math.max(size, ballot.size());
		}
		return size;
	}

	public boolean hasVoted(ID id) {
		return ids.contains(id);
	}

	public boolean add(ID id, Vote vote, Ballot ballot) {
		// Check whether 'id' has already cast a vote
		if (ids.contains(id)) return false;

		// Mark that 'id' has cast a vote
		ids.add(id);

		// Count vote
		List<Ballot> ballots = votes.get(vote);
		if (ballots == null) {
			ballots = new ArrayList<>(decisionThreshold);
			votes.put(vote, ballots);
		}
		ballots.add(ballot);

		int w = (weights != null) ? weights[id] : 1;
		voteWeights.put(vote, voteWeights.getOrDefault(vote, 0) + w);
		return true;
	}

	public Vote getDecision() {
		int voc = calcVoc();
		// Check whether there is even a chance that a decision has been reached
		if (voc < decisionThreshold) return null;

		// Check whether a decision has already been reached 
		for (Entry<Vote, List<Ballot>> entry : votes.entrySet()) {
			if (voteWeights.get(entry.getKey()) >= decisionThreshold) return entry.getKey();
		}
		return null;
	}

	protected int calcVoc() {
		int voc = 0;
		if (weights != null) {
			for (ID id : ids) {
				voc += weights[id];
			}
		} else {
			voc = ids.size();
		}
		return voc;
	}

	public List<Ballot> getDecidingBallots() {
		int voc = calcVoc();
		// Check whether there is even a chance that a decision has been reached
		if (voc < decisionThreshold) return null;

		// Check whether a decision has already been reached 
		for (Entry<Vote, List<Ballot>> entry : votes.entrySet()) {
			if (voteWeights.get(entry.getKey()) >= decisionThreshold) return entry.getValue();
		}
		return null;
	}

	public boolean votesDiffer() {
		return votes.keySet().size() > 1;
	}

}
