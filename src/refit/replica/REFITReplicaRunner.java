package refit.replica;

import java.lang.reflect.Constructor;

import refit.config.REFITConfig;


public class REFITReplicaRunner {
	public static void main(String[] args) throws Exception {
		if (args.length != 1) {
			System.err.println("usage: java " + REFITReplicaRunner.class.getSimpleName() + " <replicaID>");
			System.exit(1);
		}

		// Create and initialize replica
		short replicaID = Short.parseShort(args[0]);
		Class<? extends REFITBaseReplica> replicaType = REFITConfig.APPLICATION_REPLICA;
		Constructor<? extends REFITBaseReplica> replicaConstructor = replicaType.getConstructor(short.class, boolean.class);
		REFITBaseReplica replica = replicaConstructor.newInstance(replicaID, (REFITConfig.NR_OF_REPLICA_SCHEDULERS == 1));

		// Start replica
		replica.start();
	}
}
