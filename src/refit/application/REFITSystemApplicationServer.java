package refit.application;

public interface REFITSystemApplicationServer extends REFITApplicationServer {
	REFITLeaderRecommendation queryLeader(int lastLeader, long targetProbeCounter);
}
