package refit.application.ycsb;

import refit.client.REFITClientExternal;
import refit.client.REFITClientLibrary;
import refit.client.policy.REFITYCSBPolicy;
import refit.config.REFITConfig;
import site.ycsb.Client;


public class REFITYCSBBenchmark implements REFITClientExternal {

	// #################
	// # CONFIGURATION #
	// #################

	public static final int REQUEST_MAX = 2048;    // Maximum request size
	public static final int RESULT_MAX = 262144;   // Maximum response size
	public static final int STATE_MAX = 10485760;  // Maximum state size
	public static final String WORKLOAD = REFITConfig.YCSB_WORKLOAD;     // YCSB workload
	public static volatile boolean isFinished;

	protected static REFITClientLibrary[] ycsb;

	public REFITYCSBBenchmark(REFITClientLibrary[] clients) {
		REFITYCSBBenchmark.ycsb = clients;
	}

	public void run() {
		if (REFITConfig.APPLICATION_CLIENT_POLICY != REFITYCSBPolicy.class) {
			throw new AssertionError("YCSB requires the YCSB benchmark policy");
		}

		String[] args = new String[]{
				"-threads", String.valueOf(ycsb.length),
				"-P", "workloads/workload" + WORKLOAD,
				"-p", "measurementtype=raw",
				"-p", "measurement.raw.output_file=" + REFITConfig.OUTPUT_DIRECTORY + "/" + REFITConfig.OUTPUT_ID + ".ycsb",
				"-p", "measurement.raw.no_summary=true",
				"-db", REFITYCSBClient.class.getName(),
				"-p", "dataintegrity=true",
				"-s"
		};
		Client.main(args);
		// stop replica
		isFinished = true;
	}

	public static void main(String[] args) {
		args = new String[]{
				"-threads", String.valueOf(10),
				"-P", "workloads/workload" + WORKLOAD,
				"-p", "measurementtype=timeseries",
				"-p", "timeseries.granularity=1000",
				"-db", REFITYCSBDatabase.class.getName(),
				"-p", "dataintegrity=true",
				"-s"
		};
		Client.main(args);
	}
}
