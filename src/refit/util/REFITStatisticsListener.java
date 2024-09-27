package refit.util;


public interface REFITStatisticsListener {

	void statisticsIntervalResult(int resultIndex, int eventCount, float eventValueAverage, long eventValueMin, long eventValueMax, String tagStatistics, boolean isReject);

	void statisticsStartedResult();

	void statisticsOverallResult(int nrOfIntervals, int eventCount, float eventCountAverage, float eventValueAverage);

}
