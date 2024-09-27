package refit.scheduler;


//public class REFITSchedulerResourceUsage {
//
//	private static final ThreadMXBean THREAD_BEAN;
//	private static final com.sun.management.ThreadMXBean SUN_THREAD_BEAN;
//	static {
//		// Configure regular thread bean
//		THREAD_BEAN = ManagementFactory.getThreadMXBean();
//		THREAD_BEAN.setThreadCpuTimeEnabled(true);
//		THREAD_BEAN.setThreadContentionMonitoringEnabled(true);
//		
//		// Configure Sun's thread bean
//		boolean sunBeanAvailable = true;
//		try { com.sun.management.ThreadMXBean.class.getClass(); } catch(NoClassDefFoundError ncdfe) { sunBeanAvailable = false; }
//		if(sunBeanAvailable) {
//			SUN_THREAD_BEAN = (com.sun.management.ThreadMXBean) ManagementFactory.getThreadMXBean();
//			SUN_THREAD_BEAN.setThreadAllocatedMemoryEnabled(true);
//		} else {
//			SUN_THREAD_BEAN = null;
//		}
//		
//		if(!THREAD_BEAN.isThreadCpuTimeSupported()) REFITLogger.logWarning("RSUSG", "CPU time measuring is not supported");
//		if(!THREAD_BEAN.isThreadContentionMonitoringSupported()) REFITLogger.logWarning("RSUSG", "Contention monitoring is not supported");
//		if(SUN_THREAD_BEAN == null) REFITLogger.logWarning("RSUSG", "Memory allocation monitoring is not supported");
//	}
//
//	/* 
//	 * Note: The thread's user time is not collected as it appears to be an estimation that is
//	 *       based on the CPU time. Example of CPU time vs. user time: 155507390 vs. 150000000.
//	 */
//	
//	private final long threadID;
//	
//	private long timeStamp;        // in nanoseconds
//	private ThreadInfo threadInfo; // in milliseconds (blocked and waiting times)
//	private long cpuTime;          // in nanoseconds
//	private long userTime;         // in nanoseconds
//	private long allocatedBytes;   // in bytes
//
//	
//	public REFITSchedulerResourceUsage(long threadID) {
//		this.threadID = threadID;
//	}
//	
//	
//	public void update() {
//		this.timeStamp = REFITTime.nanoTime.getAsLong();
//		this.threadInfo = THREAD_BEAN.getThreadInfo(threadID);
//		this.cpuTime = THREAD_BEAN.getThreadCpuTime(threadID);
//		this.userTime = THREAD_BEAN.getThreadUserTime(threadID);
//		this.allocatedBytes = (SUN_THREAD_BEAN != null) ? SUN_THREAD_BEAN.getThreadAllocatedBytes(threadID) : -1L;
//	}
//
//	
//	/*
//	 * Comparison of two usage measurements.
//	 * 
//	 *   timeStats[0]: Total CPU usage in percent
//	 *   timeStats[1]: Kernel time in percent
//	 *   timeStats[2]: User time in percent
//	 *   timeStats[3]: Blocked time in percent
//	 *   timeStats[4]: Wait time in percent
//	 *   return      : Kilobyte allocation rate per second
//	 *
//	 */
//	public long calculateUsage(REFITSchedulerResourceUsage baseline, byte[] timeStats) {
//		long interval = timeStamp - baseline.timeStamp;
//		timeStats[0] = (byte) (((cpuTime - baseline.cpuTime) * 100L) / interval);
//		timeStats[1] = (byte) ((((cpuTime - userTime) - (baseline.cpuTime - baseline.userTime)) * 100L) / interval);
//		timeStats[2] = (byte) (((userTime - baseline.userTime) * 100L) / interval);
//		timeStats[3] = (byte) (((threadInfo.getBlockedTime() - baseline.threadInfo.getBlockedTime()) * 100L) / interval);
//		timeStats[4] = (byte) (((threadInfo.getWaitedTime() - baseline.threadInfo.getWaitedTime()) * 100000000L) / interval);
//		return ((allocatedBytes - baseline.allocatedBytes) * 1000000L / interval);
//	}
//	
//	public static byte[] createTimeStatsContainer() {
//		return new byte[5];
//	}
//
//	public static String timeStatsToString(byte[] timeStats) {
//		return String.format("%3d%% %3d%% %3d%% %3d%% %3d%%", timeStats[0], timeStats[1], timeStats[2], timeStats[3], timeStats[4]);
//	}
//
//}
