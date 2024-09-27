package refit.application;

public class REFITLeaderRecommendation {
	public final short leader;
	public final short[] passive;
	public final long probeCounter;
	public final short percentile;

	public REFITLeaderRecommendation(short leader, short[] passive, long probeCounter, short percentile) {
		this.leader = leader;
		this.passive = passive;
		this.probeCounter = probeCounter;
		this.percentile = percentile;
	}
}
