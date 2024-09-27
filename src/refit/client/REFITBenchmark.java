package refit.client;

import java.io.File;
import java.lang.reflect.Constructor;
import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import refit.client.policy.REFITClientPolicy;
import refit.config.REFITConfig;
import refit.scheduler.REFITHangDetector;
import refit.scheduler.REFITScheduler;
import refit.scheduler.REFITSchedulerGroup;
import refit.scheduler.REFITSchedulerTaskType;
import refit.util.*;


public class REFITBenchmark implements REFITStatisticsListener {

	private REFITIntervalStatistics statistics;
	private REFITIntervalStatistics rejectStatistics;


	public REFITBenchmark() {
		File clientLatencies = REFITConfig.CLIENT_LATENCY_STATISTICS ? new File(REFITConfig.OUTPUT_DIRECTORY + "/" + REFITConfig.OUTPUT_ID + ".latency") : null;
		statistics = new REFITIntervalStatistics(1000, REFITConfig.CLIENT_WARM_UP_SECONDS,
				REFITConfig.OUTPUT_DIRECTORY, REFITConfig.OUTPUT_ID, REFITConfig.TOTAL_NR_OF_REPLICAS, this, clientLatencies, false);
		// Separate reject statistics
		if (REFITConfig.CLIENT_SPLIT_REJECTS) {
			File rejectLatencies = REFITConfig.CLIENT_LATENCY_STATISTICS ? new File(REFITConfig.OUTPUT_DIRECTORY + "/" + REFITConfig.OUTPUT_ID + "_reject.latency") : null;
			rejectStatistics = new REFITIntervalStatistics(1000, REFITConfig.CLIENT_WARM_UP_SECONDS,
					REFITConfig.OUTPUT_DIRECTORY, REFITConfig.OUTPUT_ID + "_reject", REFITConfig.TOTAL_NR_OF_REPLICAS, this, rejectLatencies, true);
		}
	}

	@Override
	public String toString() {
		return "BENCH";
	}

	@SuppressWarnings("unchecked")
	public void runBenchmark(long durationInMs, int clientProcessID,
	                         Class<? extends REFITBenchmarkRunnable> benchmarkType,
	                         Class<? extends REFITClientLibrary> libraryType,
	                         Class<? extends REFITClientPolicy> policyType) throws InterruptedException {

		int nrOfClients = REFITConfig.NR_OF_CLIENTS[clientProcessID];
		int clientIDOffset = REFITConfig.TOTAL_NR_OF_REPLICAS;
		for (int i = 0; i < clientProcessID; i++) {
			clientIDOffset += REFITConfig.NR_OF_CLIENTS[i];
		}

		REFITLogger.logPlain("Number of clients: " + nrOfClients);
		REFITLogger.logPlain("ClientID offset: " + clientIDOffset);

		final boolean isClientTask = REFITClientTask.class.isAssignableFrom(benchmarkType);
		final boolean isClientThread = REFITClientThread.class.isAssignableFrom(benchmarkType);

		// Create schedulers
		REFITScheduler[] schedulers = new REFITScheduler[REFITConfig.NR_OF_CLIENT_SCHEDULERS];
		REFITSchedulerGroup[] groups = new REFITSchedulerGroup[schedulers.length];
		CountDownLatch startupSync = new CountDownLatch(REFITConfig.NR_OF_CLIENT_SCHEDULERS);
		REFITHangDetector hangDetector = (REFITConfig.TRACK_SCHEDULER_HANGS) ? new REFITHangDetector() : null;
		if (hangDetector != null) hangDetector.start();
		for (int i = 0; i < schedulers.length; i++) {
			schedulers[i] = new REFITScheduler(startupSync, hangDetector);
			schedulers[i].setName("CLNT" + clientProcessID + "-" + i);
			groups[i] = new REFITSchedulerGroup((REFITConfig.NR_OF_CLIENT_SCHEDULERS == 1) && isClientTask);
		}

		// Create client execution policy
		REFITClientPolicy policy = null;
		try {
			Constructor<? extends REFITClientPolicy> policyConstructor = policyType.getConstructor(long.class, int.class, int.class, int.class);
			policy = policyConstructor.newInstance(durationInMs, clientProcessID, nrOfClients, clientIDOffset);
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		}

		// Create and assign clients and client libraries
		REFITClientThread[] clients = isClientTask ? null : new REFITClientThread[nrOfClients];
		Thread[] clientThreads = isClientTask ? null : new Thread[nrOfClients];
		REFITClientTask[] clientTasks = isClientTask ? new REFITClientTask[nrOfClients] : null;
		REFITClientLibrary[] libraries = new REFITClientLibrary[nrOfClients];
		REFITClientLibrary firstLibrary = null;
		LinkedBlockingQueue<Object> sidechannel = new LinkedBlockingQueue<>();
		for (short i = 0; i < nrOfClients; i++) {
			try {
				// Create client library
				Constructor<? extends REFITClientLibrary> libraryConstructor = libraryType.getConstructor(short.class,
						REFITSchedulerGroup.class, REFITIntervalStatistics.class, REFITIntervalStatistics.class, REFITClientPolicy.class,
						boolean.class, LinkedBlockingQueue.class);
				REFITClientLibrary library = libraryConstructor.newInstance((short) (clientIDOffset + i),
						groups[i % schedulers.length], statistics, rejectStatistics, policy, i == 0, sidechannel);
				libraries[i] = library;
				if (firstLibrary == null) {
					firstLibrary = library;
				} else {
					firstLibrary.addInitialCompleteListener(library);
				}

				// Create client
				if (isClientTask) {
					Constructor<? extends REFITClientTask> benchmarkConstructor =
							(Constructor<? extends REFITClientTask>) benchmarkType.getConstructor(REFITClientLibrary.class);
					REFITClientTask client = benchmarkConstructor.newInstance(library);
					library.setClient(client);
					clientTasks[i] = client;
				} else if (isClientThread) {
					Constructor<? extends REFITClientThread> benchmarkConstructor =
							(Constructor<? extends REFITClientThread>) benchmarkType.getConstructor(REFITClientLibrary.class);
					clients[i] = benchmarkConstructor.newInstance(library);
					clientThreads[i] = new Thread(clients[i], "BENCH" + library.getNodeID());
				}
			} catch (Exception e) {
				e.printStackTrace();
				System.exit(1);
			}
		}

		// assign tasks to their schedulers
		for (int i = 0; i < schedulers.length; i++) {
			schedulers[i].assignTasks(groups[i], REFITSchedulerTaskType.NETWORK_ENDPOINT);
			schedulers[i].assignTasks(groups[i], REFITSchedulerTaskType.CLIENT_ENDPOINT_WORKER);
			schedulers[i].assignTasks(groups[i], REFITSchedulerTaskType.CLIENT_LIBRARY);
			schedulers[i].assignTasks(groups[i], REFITSchedulerTaskType.CLIENT_TASK);
			groups[i].assertAllTaskAreAssigned();
		}
		REFITLogger.logEvent(this, "Startup delay: " + REFITTime.timeSinceStart() * 0.001f + " s");

		// Start schedulers
		for (REFITScheduler scheduler : schedulers) scheduler.start();
		try {
			if (!startupSync.await(10, TimeUnit.SECONDS)) {
				System.err.println("Client scheduler startup timed out");
				System.exit(1);
			}
		} catch (InterruptedException e) {
			e.printStackTrace();
			System.exit(1);
		}

		// Start client threads
		Thread externalThread = null;
		if (isClientThread) {
			for (Thread client : clientThreads) client.start();
		} else if (!isClientTask) {
			try {
				Constructor<? extends REFITClientExternal> externalConstructor =
						(Constructor<? extends REFITClientExternal>) benchmarkType.getConstructor(REFITClientLibrary[].class);
				// wrap for varargs handling
				REFITClientExternal external = externalConstructor.newInstance(new Object[]{libraries});
				externalThread = new Thread(external);
				externalThread.start();
			} catch (Exception e) {
				e.printStackTrace();
				System.exit(1);
			}
		}

		// Wait for benchmark end
		statistics.start();
		if (REFITConfig.CLIENT_SPLIT_REJECTS) rejectStatistics.start();
		// Delay the policy start a bit to make it very likely that the policy completes a short time
		// after a statistics interval result is printed
		REFITTime.sleep(79);
		policy.execute();
		statistics.end();
		if (REFITConfig.CLIENT_SPLIT_REJECTS) rejectStatistics.end();

		if (externalThread != null) {
			externalThread.join();
		}

		REFITCallTracer.instance.dumpStacks();
		analyzeLibraryProgress(libraries);

		System.exit((lastEventCount > 25) ? 0 : 1);
	}

	private long analyzeLibraryProgress(REFITClientLibrary[] libraries) {
		long totalProgress = 0;
		long[] progresses = new long[libraries.length];
		for (int i = 0; i < libraries.length; i++) {
			long progress = libraries[i].getProgress();
			totalProgress += progress;
			progresses[i] = progress;
		}
		Arrays.sort(progresses);

		long averageProgress = totalProgress / libraries.length;
		long progressBlock = averageProgress / 20;

		long blockStart = progresses[0];
		int blockStartIdx = 0;
		REFITLogger.logPlain("=== Client progress ===");
		for (int i = 0; i < progresses.length; i++) {
			if (progresses[i] > blockStart + progressBlock) {
				int blockLen = i - blockStartIdx;
				REFITLogger.logPlain(String.format("%d - %d: %d\n", blockStart, progresses[i - 1], blockLen));
				blockStart = progresses[i];
				blockStartIdx = i;
			}
		}
		int blockLen = progresses.length - blockStartIdx;
		REFITLogger.logPlain(String.format("%d - %d: %d\n", blockStart, progresses[progresses.length - 1], blockLen));

		return totalProgress;
	}


	// ##############
	// # STATISTICS #
	// ##############

	volatile long lastEventCount = 0;

	@Override
	public void statisticsStartedResult() {
		REFITLogger.logEvent(this, "OK");
		REFITLogger.logEvent(this, String.format("Start time: %.3fs", REFITTime.timeSinceStart() * 0.001f));
	}

	@Override
	public void statisticsIntervalResult(final int resultIndex, final int eventCount, final float eventValueAverage,
	                                     final long eventValueMin, final long eventValueMax, String tagStatistics, boolean isReject) {
		REFITLogger.logPlain(String.format("%13d %4d %6d %6.0f (%6d/%6d) %s %s", REFITTime.timeMicroUTC.getAsLong(),
				resultIndex, eventCount, eventValueAverage, eventValueMin, eventValueMax, tagStatistics, isReject ? "rejects" : ""));
		if (!isReject) lastEventCount = eventCount;
		if (REFITConfig.CLIENT_REPLY_STATISTICS) {
			statistics.printReplyStats();
			if (REFITConfig.CLIENT_SPLIT_REJECTS) rejectStatistics.printReplyStats();
		}
	}

	@Override
	public void statisticsOverallResult(int nrOfIntervals, int eventCount, float eventCountAverage, float eventValueAverage) {
		REFITLogger.logEvent(this, String.format("END: %6d %6.0f %6.0f (%3d)", eventCount, eventCountAverage, eventValueAverage, nrOfIntervals));
	}


	// ########
	// # MAIN #
	// ########

	@SuppressWarnings("unchecked")
	public static void main(String[] args) throws Exception {
		if (args.length < 2) {
			System.err.println("usage: java " + REFITBenchmark.class.getSimpleName() + " <duration> <client-group-id> [<start-delay>]");
			System.exit(1);
		}

		long durationInMs = Integer.parseInt(args[0]) * 1000;
		int clientProcessID = Integer.parseInt(args[1]);

		Class<? extends REFITBenchmarkRunnable> benchmarkType = REFITConfig.APPLICATION_CLIENT;
		Class<? extends REFITClientLibrary> libraryType = REFITConfig.APPLICATION_CLIENT_LIBRARY;
		Class<? extends REFITClientPolicy> policyType = REFITConfig.APPLICATION_CLIENT_POLICY;

		REFITBenchmark benchmark = new REFITBenchmark();
		benchmark.runBenchmark(durationInMs, clientProcessID, benchmarkType, libraryType, policyType);
	}

}
