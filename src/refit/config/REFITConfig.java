package refit.config;

import java.io.File;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import refit.application.REFITApplicationServer;
import refit.application.REFITSystemApplicationServer;
import refit.client.REFITBenchmarkRunnable;
import refit.client.REFITClientLibrary;
import refit.client.policy.REFITClientPolicy;
import refit.replica.REFITBaseReplica;
import refit.util.REFITAssert;
import refit.util.REFITLogger;


public class REFITConfig {
	private static final REFITProperties config;

	static {
		REFITProperties defaults = REFITProperties.loadFile("scripts/config/refit-defaults");
		config = REFITProperties.loadFile("scripts/config/refit-overrides", defaults);
	}

	// Just for numbering experiment repetitions
	public static final int RUN = config.getInt("run_number");

	// Cell
	public static final boolean BYZANTINE = config.getBoolean("system.bft");
	public static final short FAULTS_TO_TOLERATE = config.getShort("system.faults");
	public static final short TOTAL_NR_OF_REPLICAS = config.getShort("replica.count");
	public static final short TOTAL_NR_OF_CLIENTS = config.getShort("replica.max_clients");
	public static final short[] NR_OF_CLIENTS = config.getShortArray("client.count");
	public static final boolean CLIENT_MULTICAST = config.getBoolean("client.multicast");

	static {
		int sum = 0;
		for (short nr : NR_OF_CLIENTS) {
			sum += nr;
		}
		if (sum > TOTAL_NR_OF_CLIENTS) {
			throw new AssertionError("Trying to use more clients than available");
		}
	}

	// Intervals and timeouts
	public static final int CLIENT_REQUEST_TIMEOUT = config.getInt("client.timeout.request");
	public static final int REPLICA_REQUEST_TIMEOUT = config.getInt("replica.timeout.request");
	public static final int REPLICA_PANIC_INTERVAL = config.getInt("replica.timeout.panic_interval");

	// Client-replica & replica-replica communication
	public static final boolean AUTHENTICATE_MESSAGES = config.getBoolean("system.crypto.authenticate");
	public static final boolean USE_ED25519 = config.getBoolean("system.crypto.ed25519");
	public static final int RSA_SIGNATURE_SIZE = config.getInt("system.crypto.rsa_signature_size");
	public static final boolean SIGNED_REQUESTS = config.getBoolean("system.signed_requests");
	public static final boolean USE_HASHED_REPLIES = config.getBoolean("client.replies.hashed");
	public static final boolean USE_PBFT_READ_OPTIMIZATION = config.getBoolean("client.pbft_read_opt");
	public static final boolean AUTONOMOUS_PANICS = config.getBoolean("replica.client.autonomous_panics");

	public static final boolean FLOW_CONTROL = config.getBoolean("replica.client.flow_control");

	// Checkpoints
	public static final int CHECKPOINT_INTERVAL = config.getInt("replica.checkpoint.interval");
	// with signatures only f+1 checkpoints are required or when using the upright trick
	public static final int REGULAR_CHECKPOINT_STABILITY_THRESHOLD;
	static {
		int checkpointThreshold = config.getBoolean("replica.checkpoint.quick_stability")
				? REFITConfig.FAULTS_TO_TOLERATE + 1 : 2 * REFITConfig.FAULTS_TO_TOLERATE + 1;
		if (!AUTHENTICATE_MESSAGES || !BYZANTINE) checkpointThreshold = 1;
		REGULAR_CHECKPOINT_STABILITY_THRESHOLD = checkpointThreshold;
	}

	// Order stage
	public static final int INITIAL_VIEW = config.getInt("replica.order.initial_view");
	public static final int MAXIMUM_ORDER_INSTANCES_IN_PROGRESS = (int) (CHECKPOINT_INTERVAL * config.getFloat("replica.order.instance_factor"));
	public static final int INSTANCES_AFTER_CHECKPOINT = (int) (CHECKPOINT_INTERVAL * config.getFloat("replica.order.instance_slack_factor"));
	public static final int MINIMUM_BATCH_SIZE = config.getInt("replica.order.min_batch");
	public static final int[] MAXIMUM_BATCH_SIZE = config.getIntArray("replica.order.max_batch");
	public static final int MAXIMUM_BATCH_BYTES = config.getInt("replica.order.max_batch_bytes");
	public static final boolean PREFER_FULL_BATCHES = config.getBoolean("replica.order.prefer_full_batches");
	public static final int INCOMPLETE_BATCH_DELAY = config.getInt("replica.order.incomplete_batch_delay");
	public static final boolean BATCH_ALWAYS = config.getBoolean("replica.order.batch_always");
	public static final int ACTIVE_PROPOSALS = config.getInt("replica.order.active_proposals");
	public static final boolean PBFT_SIGNATURES = config.getBoolean("replica.order.pbft.signatures");

	static {
		if (INSTANCES_AFTER_CHECKPOINT <= CHECKPOINT_INTERVAL || INSTANCES_AFTER_CHECKPOINT > MAXIMUM_ORDER_INSTANCES_IN_PROGRESS) {
			throw new AssertionError("Invalid order window size");
		}
	}

	// Checkpoints, depends on order instance count
	public static final int MAX_CHECKPOINTS = REFITConfig.MAXIMUM_ORDER_INSTANCES_IN_PROGRESS / REFITConfig.CHECKPOINT_INTERVAL + 1;

	// Idem
	public static final int FORWARD_TIMEOUT = config.getInt("replica.idem.forward_timeout");
	public static final int REJECT_GRACE = config.getInt("replica.idem.reject_grace_period");
	public static final int REJECT_THRESHOLD = config.getInt("replica.idem.reject_threshold");
	public static final int REJECT_MAX_IDLE = config.getInt("replica.idem.reject_max_idle");
	public static final int REJECT_TIMEOUT = config.getInt("replica.idem.reject_timeout");
	public static final boolean ACTIVE_QUEUE_MANAGEMENT = config.getBoolean("replica.idem.active_queue_management");
	public static final int CLIENT_GROUPS = (REJECT_THRESHOLD != 0 && REJECT_THRESHOLD < NR_OF_CLIENTS[0]) ? NR_OF_CLIENTS[0] / REJECT_THRESHOLD : 1;
	public static final int CLIENT_TIME_SLICE = config.getInt("replica.idem.time_slice");

	static {
		if (ACTIVE_QUEUE_MANAGEMENT && CLIENT_TIME_SLICE == 0) {
			throw new AssertionError("Client time slice must be set when active queue management is enabled!");
		}
	}

	public static int currentClientGroup() {
		if (CLIENT_GROUPS == 1) {
			return 1;
		} else {
			return Math.abs((int) (System.currentTimeMillis() / CLIENT_TIME_SLICE) % CLIENT_GROUPS);
		}
	}

	static {
		if (FLOW_CONTROL && MAXIMUM_ORDER_INSTANCES_IN_PROGRESS < (REJECT_THRESHOLD * TOTAL_NR_OF_REPLICAS)) {
			throw new AssertionError("Maximum number of active allowed requests must be smaller or equal to window size");
		}
	}

	// Debugging and statistics
	public static final boolean ENABLE_DEBUG_CHECKS = config.getBoolean("system.debug_checks");
	public static final boolean TRACE_AUTH = config.getBoolean("system.trace_auth");
	public static final boolean TRACE_MESSAGES = config.getBoolean("system.trace_messages");
	public static final boolean TRACK_SCHEDULER_HANGS = config.getBoolean("system.track_scheduler_hangs");
	public static final boolean TRACK_SLOW_STAGES = config.getBoolean("system.track_slow_stages");
	public static final boolean COLLECT_STAGE_STATISTICS = config.getBoolean("system.statistics.stage");
	public static final boolean CLIENT_REPLY_STATISTICS = config.getBoolean("client.statistics.repliers");
	public static final boolean CLIENT_LATENCY_STATISTICS = config.getBoolean("client.statistics.latency");
	public static final int CLIENT_WARM_UP_SECONDS = config.getInt("client.statistics.warm_up_seconds");
	public static final boolean CLIENT_SPLIT_REJECTS = config.getBoolean("client.statistics.split_rejects");

	// Schedulers
	public static final int NR_OF_CLIENT_SCHEDULERS = config.getInt("client.schedulers");
	public static final int NR_OF_REPLICA_SCHEDULERS = config.getInt("replica.schedulers");

	// Benchmark workload
	public static final int REQUEST_SIZE = config.getInt("application.request_size");
	public static final int REPLY_SIZE = config.getInt("application.reply_size");
	public static final int NR_OF_NODES = config.getInt("application.nodes");
	public static final int ACTIVE_NODES = (int) (NR_OF_NODES * config.getFloat("application.active_factor"));
	// enable updating of last access field in REZK on getData requests
	public static final boolean UPDATE_LAST_ACCESS = config.getBoolean("application.update_last_access");
	// 0 = write-only, 1 = read-only
	public static final float READ_WRITE_RATIO = config.getFloat("application.read_ratio");
	public static final boolean WAIT_FOR_FULL_REPLY = config.getBoolean("application.wait_write");
	public static final String YCSB_WORKLOAD = config.getString("application.ycsb_workload");

	public static final boolean ONLY_CONSISTENT = config.getBoolean("application.only_consistent");
	// Benchmark client execution policy
	public static final int[] POLICY_OPEN_LOOP_RATE = config.getIntArray("policy.open_loop.rate");
	public static final int POLICY_OPEN_LOOP_SLOW_START = config.getInt("policy.open_loop.slow_start_in_ms");

	// Benchmark applications
	// The benchmark classes MUST be loaded after the benchmark parameters, otherwise these can't be
	// accessed during initialization
	public static final Class<? extends REFITApplicationServer> APPLICATION_SERVER = config.getClass("application.server", REFITApplicationServer.class);
	public static final Class<? extends REFITSystemApplicationServer> SYSTEM_APPLICATION_SERVER = config.getClass("application.system.server", REFITSystemApplicationServer.class);
	public static final Class<? extends REFITBenchmarkRunnable> APPLICATION_CLIENT = config.getClass("application.client", REFITBenchmarkRunnable.class);
	public static final Class<? extends REFITClientLibrary> APPLICATION_CLIENT_LIBRARY = config.getClass("application.library", REFITClientLibrary.class);
	public static final Class<? extends REFITClientPolicy> APPLICATION_CLIENT_POLICY = config.getClass("application.policy", REFITClientPolicy.class);
	public static final Class<? extends REFITBaseReplica> APPLICATION_REPLICA;

	// Communication
	public static final int REPLICA_NETWORK_SEND_BUFFER_SIZE = 1024 * config.getInt("replica.network.buffer.send.kb");
	public static final int REPLICA_NETWORK_RECEIVE_BUFFER_SIZE = 1024 * config.getInt("replica.network.buffer.receive.kb");
	public static final int REPLICA_NETWORK_CONNECTORS = config.getInt("replica.network.connectors");
	public static final int SERVER_SOCKET_BACKLOG = config.getInt("replica.network.socket_backlog");
	public static final int CLIENT_NETWORK_RECEIVE_BUFFER_SIZE = 1024 * config.getInt("replica.network.buffer.client.kb");
	public static final int CLIENT_RECEIVE_BUFFER_SIZE = 1024 * config.getInt("client.network.buffer.receive.kb");
	public static final int CLIENT_SEND_BUFFER_SIZE = 1024 * config.getInt("client.network.buffer.send.kb");
	// use _per_ client
	public static final int CLIENT_INDIVIDUAL_NETWORK_CONNECTORS = config.getInt("client.network.individual_connectors");

	// (Log) output directory
	public static final String OUTPUT_DIRECTORY = REFITSystemHelper.getenv("OUTPUT_DIRECTORY", "tmp");
	// (Log) file prefix name
	public static final String OUTPUT_ID = REFITSystemHelper.getenv("OUTPUT_ID", "client-0");

	static {
		// ensure output directory exists
		//noinspection ResultOfMethodCallIgnored
		new File(OUTPUT_DIRECTORY).mkdir();
	}

	// #########################
	// # NETWORK CONFIGURATION #
	// #########################

	public static final SocketAddress[] ADDRESSES = new SocketAddress[TOTAL_NR_OF_REPLICAS + TOTAL_NR_OF_CLIENTS];
	public static final SocketAddress[] INT_ADDRESSES = new SocketAddress[TOTAL_NR_OF_REPLICAS + TOTAL_NR_OF_CLIENTS];
	public static final SocketAddress[] BIND_ADDRESSES = new SocketAddress[TOTAL_NR_OF_REPLICAS];
	public static final int[] LOCATION = new int[TOTAL_NR_OF_REPLICAS + TOTAL_NR_OF_CLIENTS];

	private static final String[] CLIENT_RAW_ADDRESSES = config.getStringArray("client.network.addresses");
	private static final String[] SERVER_RAW_LIST = config.getStringArray("replica.network.addresses");
	private static final String BIND_RAW_ADDRESS = config.getString("replica.network.bind_address");

	static {
		REFITProperties serversMap = REFITProperties.loadFile("scripts/config/servers");

		REFITAssert.assertTrue(SERVER_RAW_LIST.length >= TOTAL_NR_OF_REPLICAS, "Not enough replicas");
		boolean isLocalSetup = true;
		for (int i = 0; i < TOTAL_NR_OF_REPLICAS; i++) {
			String rawAddress = serversMap.getString(SERVER_RAW_LIST[i]);
			ADDRESSES[i] = new InetSocketAddress(rawAddress, 8170 + i);
			String intAddress = serversMap.getOptionalString(SERVER_RAW_LIST[i] + "int");
			INT_ADDRESSES[i] = (intAddress != null) ? new InetSocketAddress(intAddress, 8170 + i) : ADDRESSES[i];
			BIND_ADDRESSES[i] = new InetSocketAddress(BIND_RAW_ADDRESS, 8170 + i);
			String loc = serversMap.getOptionalString(SERVER_RAW_LIST[i] + "loc");
			LOCATION[i] = (loc != null) ? Integer.parseInt(loc) : 0;

			if (!rawAddress.equals("localhost") || (intAddress != null && !intAddress.equals("localhost")))
				isLocalSetup = false;
		}
		if (!isLocalSetup) {
			if (REFITConfig.TRACE_AUTH || REFITConfig.TRACE_MESSAGES) {
				REFITLogger.logWarning("CONFIG", "Message or auth tracing is slow!");
			}
		}

		for (String clientRawAddress : CLIENT_RAW_ADDRESSES) {
			serversMap.getString(clientRawAddress);
		}

		for (int i = 0; i < (ADDRESSES.length - TOTAL_NR_OF_REPLICAS); i++) {
			ADDRESSES[i + TOTAL_NR_OF_REPLICAS] = new InetSocketAddress(12345 + i);
		}

		int ofs = TOTAL_NR_OF_REPLICAS;
		for (int i = 0; i < NR_OF_CLIENTS.length; ++i) {
			String loc = serversMap.getOptionalString(CLIENT_RAW_ADDRESSES[i] + "loc");
			for (int j = 0; j < NR_OF_CLIENTS[i]; ++j) {
				LOCATION[ofs + j] = (loc != null) ? Integer.parseInt(loc) : 0;
			}
			ofs += NR_OF_CLIENTS[i];
		}

		Set<Integer> locations = new HashSet<>();
		int maxLoc = -1;
		for (int value : LOCATION) {
			REFITAssert.assertTrue(value >= 0 && value < 16, "Location value must be between 0 and 16 (exclusive)");
			maxLoc = Math.max(maxLoc, value);
			locations.add(value);
		}

		REFITAssert.assertTrue(!REFITConfig.AUTHENTICATE_MESSAGES || REFITConfig.WAIT_FOR_FULL_REPLY,
				"ack replies are not yet implemented in the BFT protocol");

		APPLICATION_REPLICA = config.getClass("application.replica", REFITBaseReplica.class);
	}


	private static final Map<SocketAddress, Short> addresses = new HashMap<>();

	static {
		for (short i = 0; i < ADDRESSES.length; i++) {
			addresses.put(ADDRESSES[i], i);
		}
		for (short i = 0; i < INT_ADDRESSES.length; i++) {
			addresses.put((INT_ADDRESSES[i] != null) ? INT_ADDRESSES[i] : ADDRESSES[i], i);
		}
	}

	public static short getNodeByAddress(SocketAddress address) {
		return addresses.get(address);
	}

	static {
		config.assertNoUnusedKeys();
	}
}
