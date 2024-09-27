package refit.util;


public class REFITLogger {

	public static final boolean DISABLE_LOG = true;

	public static final boolean LOG_EVENT = DISABLE_LOG ? true : true; // DO NOT DISABLE!!! Analysis does not work then!
	public static final boolean LOG_COMMUNICATION = DISABLE_LOG ? false : false;

	public static final boolean LOG_CLIENT = DISABLE_LOG ? false : false;
	public static final boolean LOG_PANIC = DISABLE_LOG ? false : false;
	public static final boolean LOG_ORDER = DISABLE_LOG ? false : false;
	public static final boolean LOG_EXECUTION = DISABLE_LOG ? false : false;
	public static final boolean LOG_CHECKPOINT = DISABLE_LOG ? false : false;

	public static final boolean LOG_WARNING = false;

	private static void logNormal(String type, Object component, String message) {
		String str = REFITTime.timeMicroUTC.getAsLong() + " " + Thread.currentThread().getName() + " " + type + " " + component + ": " + message;
		synchronized (REFITLogger.class) {
			System.out.println(str);
		}
	}

	private static void logError(String type, Object component, String message) {
		String str = REFITTime.timeMicroUTC.getAsLong() + " " + Thread.currentThread().getName() + " " + type + " " + component + ": " + message;
		synchronized (REFITLogger.class) {
			System.err.println(str);
		}
	}


	// ###########
	// # GENERAL #
	// ###########

	public static void logDebug(Object component, String message) {
		logError("[DEBUG]", component, message);
	}

	public static void logWarning(Object component, String message) {
		if (LOG_WARNING) logError("[WARNG]", component, message);
	}

	public static void logError(Object component, String message) {
		logError("[ERROR]", component, message);
	}

	public static void logPlain(String message) {
		synchronized (REFITLogger.class) {
			System.out.println(message);
		}
	}

	// ############
	// # SPECIFIC #
	// ############

	public static void logEvent(Object component, String message) {
		if (LOG_EVENT) logNormal("[EVENT]", component, message);
	}

	public static void logCommunication(Object component, String message) {
		if (LOG_COMMUNICATION) logNormal("[CMNCT]", component, message);
	}

	public static void logClient(Object component, String message) {
		if (LOG_CLIENT) logNormal("[CLINT]", component, message);
	}

	public static void logPanic(Object component, String message) {
		if (LOG_PANIC) logNormal("[PANIC]", component, message);
	}

	public static void logOrder(Object component, String message) {
		if (LOG_ORDER) logNormal("[ORDER]", component, message);
	}

	public static void logExecution(Object component, String message) {
		if (LOG_EXECUTION) logNormal("[EXECT]", component, message);
	}

	public static void logCheckpoint(Object component, String message) {
		if (LOG_CHECKPOINT) logNormal("[CHKPT]", component, message);
	}
}
