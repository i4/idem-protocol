package refit.client;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import refit.client.policy.REFITClientPolicy;
import refit.config.REFITConfig;
import refit.replica.REFITBaseReplica;
import refit.util.REFITTime;

public class REFITLocalSystem {

	public static void main(String[] args) throws Exception {
		if (args.length < 1 || args.length > 2) {
			System.err.println("usage: java " + REFITLocalSystem.class.getSimpleName() + " <duration> [<killReplicaWithId>]");
			System.exit(1);
		}

		// spawn replicas
		REFITBaseReplica[] replicas = new REFITBaseReplica[REFITConfig.TOTAL_NR_OF_REPLICAS];

		Class<? extends REFITBaseReplica> replicaType = REFITConfig.APPLICATION_REPLICA;
		Constructor<? extends REFITBaseReplica> replicaConstructor = replicaType.getConstructor(short.class, boolean.class);

		ExecutorService executor = Executors.newFixedThreadPool(8);
		for (short i = 0; i < replicas.length; i++) {
			final short repID = i;
			executor.execute(() -> {
				try {
					// Create and initialize replica
					REFITBaseReplica replica = replicaConstructor.newInstance(repID, (REFITConfig.NR_OF_REPLICA_SCHEDULERS == 1));
					replicas[repID] = replica;
				} catch (InstantiationException | IllegalAccessException | InvocationTargetException e) {
					throw new InternalError(e);
				}
			});
			if (i == 0) REFITTime.sleep(100);
		}
		executor.shutdown();
		try {
			boolean isComplete = executor.awaitTermination(60, TimeUnit.SECONDS);
			if (!isComplete) {
				System.err.println("Failed to initialize replicas in time");
				System.exit(1);
			}
		} catch (InterruptedException e) {
			e.printStackTrace();
			System.exit(1);
		}

		// enforce that exceptions are handled
		Thread.setDefaultUncaughtExceptionHandler((t, e) -> {
			System.err.println("Unhandled exception: " + e + " in Thread " + t);
			e.printStackTrace();
			System.exit(1);
		});

		for (REFITBaseReplica replica : replicas) {
			replica.start();
		}

		int killReplicaWithId = (args.length == 2) ? Integer.parseInt(args[1]) : -1;
		if (killReplicaWithId >= REFITConfig.TOTAL_NR_OF_REPLICAS) {
			System.err.println("Invalid replica id");
			System.exit(1);
		} else if (killReplicaWithId >= 0) {
			// kill replica
			new Thread(() -> {
				try {
					REFITTime.sleep(5 * 1000);
					replicas[killReplicaWithId].stop();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}).start();
		}

		long durationInMs = Integer.parseInt(args[0]) * 1000L;

		REFITBenchmark[] benchmarks = new REFITBenchmark[REFITConfig.NR_OF_CLIENTS.length];
		Thread[] bt = new Thread[benchmarks.length];

		for (int i = 0; i < benchmarks.length; i++) {
			// spawn clients
			Class<? extends REFITBenchmarkRunnable> benchmarkType = REFITConfig.APPLICATION_CLIENT;
			Class<? extends REFITClientLibrary> libraryType = REFITConfig.APPLICATION_CLIENT_LIBRARY;
			Class<? extends REFITClientPolicy> policyType = REFITConfig.APPLICATION_CLIENT_POLICY;

			final int idx = i;
			bt[i] = new Thread(() -> {
				benchmarks[idx] = new REFITBenchmark();
				try {
					benchmarks[idx].runBenchmark(durationInMs, idx, benchmarkType, libraryType, policyType);
				} catch (InterruptedException e) {
					e.printStackTrace();
					System.exit(1);
				}
			});
			bt[i].setName("benchmark-" + idx);
			bt[i].start();
		}

		for (Thread thread : bt) {
			thread.join();
		}
	}
}
