package refit.util;


import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintStream;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.concurrent.atomic.AtomicLong;

import org.HdrHistogram.ConcurrentHistogram;
import org.HdrHistogram.Histogram;
import org.HdrHistogram.WriterReaderPhaser;

public class REFITIntervalStatistics extends Thread {

	private static final String WARMUP_HISTOGRAM_SUFFIX = "-warmup.histogram";
	private static final String HISTOGRAM_SUFFIX = ".histogram";

	private final int intervalInMs;
	private final REFITStatisticsListener listener;
	private final int warmupDurationInSeconds;
	private final String histOutputDirectory;
	private final String histOutputID;

	private volatile boolean running;
	private volatile EventCounters event;
	private final WriterReaderPhaser eventPhaser;
	private final AtomicBoolean firstEvent;

	public int lastEpochCounter;
	public long overallDurationInUs;
	public int overallEventCounter;
	public long overallEventValueAggregation;

	private AtomicIntegerArray replyStats;

	private final REFITDataLogger dataLogger;
	private final Histogram warmupHistogram;
	private final Histogram histogram;

	private final boolean rejectStatistics;

	private ConcurrentHashMap<ResultType, AtomicInteger> typeCounters;

	public enum ResultType {
		ACCEPT,
		ACCEPT_REJECT,
		REJECT_TIMEOUT,
		FULL_REJECT
	}

	public REFITIntervalStatistics(int intervalInMs, int warmupDurationInSeconds,
	                               String histOutputDirectory, String histOutputID, int nrOfReplicas,
	                               REFITStatisticsListener listener, File dataSink, boolean rejectStatistics) {
		setName("STATS-" + histOutputID);

		this.intervalInMs = intervalInMs;
		this.warmupDurationInSeconds = warmupDurationInSeconds;
		this.histOutputDirectory = histOutputDirectory;
		this.histOutputID = histOutputID;
		this.listener = listener;

		running = false;
		event = null;
		eventPhaser = new WriterReaderPhaser();
		firstEvent = new AtomicBoolean(true);

		this.replyStats = new AtomicIntegerArray(nrOfReplicas);

		this.dataLogger = (dataSink != null) ? new REFITDataLogger(dataSink) : null;

		this.warmupHistogram = new ConcurrentHistogram(200 * 1000 * 1000, 2);
		this.histogram = new ConcurrentHistogram(200 * 1000 * 1000, 2);

		this.rejectStatistics = rejectStatistics;
		this.typeCounters = new ConcurrentHashMap<ResultType, AtomicInteger>();
	}


	@Override
	public void run() {
		Thread logThread = null;
		if (dataLogger != null) {
			logThread = new Thread(dataLogger, "DATALOGGER-" + histOutputID);
			logThread.start();
		}

		long statisticsStartTime = REFITTime.nanoTime.getAsLong() / 1000;
		int nextEpochCounter = 1;

		eventPhaser.readerLock();
		// initialize event object
		event = new EventCounters(nextEpochCounter);
		eventPhaser.flipPhase();
		eventPhaser.readerUnlock();

		running = true;

		while (running) {
			// compensate thread timing drift
			int timePassed = (int) ((REFITTime.nanoTime.getAsLong() / 1000 - statisticsStartTime) / 1000);
			int remainingSleepTime = nextEpochCounter * intervalInMs - timePassed;
			if (nextEpochCounter < 30 && remainingSleepTime <= 0) {
				// ignore time jumps during startup
				remainingSleepTime = 1;
			}
			// May raise an error if the time jumps
			try {
				REFITTime.sleep(remainingSleepTime);
			} catch (final InterruptedException ignored) {
			}
			nextEpochCounter++;

			eventPhaser.readerLock();
			// replace event object
			EventCounters oldEvent = event;
			event = new EventCounters(nextEpochCounter);
			ConcurrentHashMap<ResultType, AtomicInteger> oldTypeCounters = typeCounters;
			typeCounters = new ConcurrentHashMap<ResultType, AtomicInteger>();

			// wait until all writers have finished
			eventPhaser.flipPhase();

			// extract data
			int myResultIndex = oldEvent.epochCounter;
			int myEventCount = oldEvent.counter.get();
			long myEventValueAggregation = oldEvent.valueAggregation.get();
			long myEventValueMin = oldEvent.valueMin.get();
			long myEventValueMax = oldEvent.valueMax.get();
			eventPhaser.readerUnlock();

			String tagStatistics = "[";
			for (Entry<ResultType, AtomicInteger> e : oldTypeCounters.entrySet()) {
				tagStatistics += e.getKey() + ": " + e.getValue().get() + ", ";
			}
			tagStatistics += "]";


			// don't print incomplete last interval
			if (!running) {
				break;
			}

			// update overall counters
			lastEpochCounter = myResultIndex;
			overallEventCounter += myEventCount;
			overallEventValueAggregation += myEventValueAggregation;

			float myEventValueAverage = oldEvent.valueAggregation.get() / (float) myEventCount;
			listener.statisticsIntervalResult(myResultIndex, myEventCount, myEventValueAverage,
					myEventValueMin, myEventValueMax, tagStatistics, rejectStatistics);
		}
		long statisticsEndTime = REFITTime.nanoTime.getAsLong() / 1000;
		overallDurationInUs = statisticsEndTime - statisticsStartTime;

		if (logThread != null) {
			dataLogger.shutdown();
			try {
				logThread.join();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

	private static void dumpHistogram(Histogram histogram, String fileName) {
		try (PrintStream output = new PrintStream(fileName)) {
			histogram.outputPercentileDistribution(output, 1.);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
	}

	private static void printShortStatistic(Histogram histogram) {
		double[] steps = {0., 0.25, 0.5, 0.75, 0.99, 1.};
		for (double step : steps) {
			long value = histogram.getValueAtPercentile(step * 100);
			REFITLogger.logPlain(String.format("Percentile %d%%: %f ms\n", (int) (step * 100), value / 1000.));
		}
	}


	public void end() {
		running = false;

		try {
			join();
		} catch (InterruptedException ignored) {
		}

		// make sure that all threads have left the critical section of the event method
		eventPhaser.readerLock();
		eventPhaser.flipPhase();
		eventPhaser.readerUnlock();

		listener.statisticsOverallResult(lastEpochCounter,
				overallEventCounter,
				overallEventCounter / (float) (overallDurationInUs / (intervalInMs * 1000)),
				overallEventValueAggregation / (float) overallEventCounter);

		dumpHistogram(warmupHistogram, histOutputDirectory + "/" + histOutputID + WARMUP_HISTOGRAM_SUFFIX);
		dumpHistogram(histogram, histOutputDirectory + "/" + histOutputID + HISTOGRAM_SUFFIX);

		REFITLogger.logPlain("=== Warmup histogram ===");
		printShortStatistic(warmupHistogram);
		REFITLogger.logPlain("=== Histogram ===");
		printShortStatistic(histogram);
	}

	public void event(final long value, final short id, String tag, ResultType rt) {
		final long currentTime = REFITTime.timeMicroUTC.getAsLong();
		if (dataLogger != null && running) dataLogger.log(id + " " + currentTime + " " + value + " " + tag);

		AtomicInteger tc = typeCounters.get(rt);
		if (tc == null) {
			tc = new AtomicInteger();
			typeCounters.put(rt, tc);
		}
		tc.incrementAndGet();

		// Use phaser the ensure non-blocking statistic updates
		long enterVal = eventPhaser.writerCriticalSectionEnter();

		// Drop events while not running
		// we must enter the critical section before checking `running` to make sure that `end` can wait until no-one
		// is any longer in this section after `running` is reset to false
		if (!running) {
			eventPhaser.writerCriticalSectionExit(enterVal);
			return;
		}

		EventCounters event = this.event;
		boolean firstEvent = this.firstEvent.getAndSet(false);
		int epochCounter = event.epochCounter;

		event.counter.incrementAndGet();
		event.valueAggregation.addAndGet(value);
		event.valueMin.accumulateAndGet(value, Math::min);
		event.valueMax.accumulateAndGet(value, Math::max);

		Histogram histogram = (epochCounter > warmupDurationInSeconds) ? this.histogram : this.warmupHistogram;
		histogram.recordValue(value);

		eventPhaser.writerCriticalSectionExit(enterVal);

		if (firstEvent) {
			listener.statisticsStartedResult();
		}

	}

	public void trackReply(short from) {
		replyStats.incrementAndGet(from);
	}

	public void printReplyStats() {
		for (int i = 0; i < replyStats.length(); i++) {
			final short key = (short) i;
			final int count = replyStats.getAndSet(i, 0);
			REFITLogger.logPlain(String.format("Replica %3d:  %7d\n", key, count));
		}
	}

	private static class EventCounters {
		public final AtomicInteger counter;
		public final AtomicLong valueAggregation;
		public final AtomicLong valueMin;
		public final AtomicLong valueMax;

		public final int epochCounter;

		public EventCounters(int newEpochCount) {
			counter = new AtomicInteger(0);
			valueAggregation = new AtomicLong(0);
			valueMin = new AtomicLong(Long.MAX_VALUE);
			valueMax = new AtomicLong(Long.MIN_VALUE);

			epochCounter = newEpochCount;
		}
	}

}
