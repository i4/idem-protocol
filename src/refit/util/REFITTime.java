package refit.util;

import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.time.Clock;
import java.time.Instant;
import java.util.function.LongSupplier;

public class REFITTime {
	// the debug version of this function does not exhibit time jumps when the process is paused during debugging
	public static final LongSupplier currentTimeMillis;
	public static final LongSupplier nanoTime;
	public static final LongSupplier timeMicroUTC;

	static {
		// The debugger detection is currently intellij specific
		RuntimeMXBean runtimeMXBean = ManagementFactory.getRuntimeMXBean();
		boolean isDebugged = runtimeMXBean.getInputArguments().stream().anyMatch((s) -> s.endsWith("debugger-agent.jar"));
		if (isDebugged) {
			currentTimeMillis = REFITTime::debugCurrentTimeMillis;
			nanoTime = REFITTime::debugNanoTime;
			timeMicroUTC = REFITTime::debugTimeMicro;
			startDebugTimer();
		} else {
			currentTimeMillis = System::currentTimeMillis;
			nanoTime = System::nanoTime;
			timeMicroUTC = REFITTime::normalTimeMicro;
		}
	}

	public static long timeSinceStart() {
		RuntimeMXBean runtimeMXBean = ManagementFactory.getRuntimeMXBean();
		long startTime = runtimeMXBean.getStartTime();
		// use the real currentTime as startTime is also given in realtime
		return System.currentTimeMillis() - startTime;
	}

	private static volatile long debugTime = System.currentTimeMillis();

	private static volatile long preVersion = 0;
	private static volatile long lastNanoTime = System.nanoTime();
	private static volatile long skippedNanoTime = 0;
	private static volatile long lastMicroUTCTime;
	private static volatile long skippedMicroUTCTime = 0;
	private static volatile long postVersion = 0;

	static {
		Instant i = Clock.systemUTC().instant();
		lastMicroUTCTime = i.getEpochSecond() * 1000 * 1000 + i.getNano() / 1000;
	}

	private static final long MAX_TIMER_DELAY_MS = 5;

	private static long debugCurrentTimeMillis() {
		return debugTime;
	}

	private static long debugNanoTime() {
		// ensure to get matching lastNanoTime and skippedNanoTime;
		long localLastNanoTime, localSkippedNanoTime;
		long localPreVersion, localPostVersion;
		do {
			localPreVersion = preVersion;
			localLastNanoTime = lastNanoTime;
			localSkippedNanoTime = skippedNanoTime;
			localPostVersion = postVersion;
		} while (localPreVersion != localPostVersion);

		// read nanoTime after lastNanoTime to ensure that the former will always have a larger value than the latter
		// reading the nanoTime before would require adding 'nanoTime = Math.max(nanoTime, localLastNanoTime)'
		// to ensure we don't mix up an old nanoTime with a newer lastNanoTime and skippedNanoTime
		// nanoTime will report a too low value if this call is paused between the loop and the call to System.nanoTime
		// it would probably be possible to handle this somehow. However, it's probably not worth the effort and the
		// more complicated and slower code
		long nanoTime = System.nanoTime();
		// we will loose time if more than 5ms pass between two activations of the debug timer
		return Math.min(nanoTime, localLastNanoTime + MAX_TIMER_DELAY_MS * 1000 * 1000) - localSkippedNanoTime;
	}

	private static long normalTimeMicro() {
		Instant time = Clock.systemUTC().instant();
		return time.getEpochSecond() * 1000 * 1000 + time.getNano() / 1000;
	}

	private static long debugTimeMicro() {
		// see debugNanoTime()
		long localLastMicroUTCTime, localSkippedMicroUTCTime;
		long localPreVersion, localPostVersion;
		do {
			localPreVersion = preVersion;
			localLastMicroUTCTime = lastMicroUTCTime;
			localSkippedMicroUTCTime = skippedMicroUTCTime;
			localPostVersion = postVersion;
		} while (localPreVersion != localPostVersion);

		long microUTCTime = normalTimeMicro();
		return Math.min(microUTCTime, localLastMicroUTCTime + MAX_TIMER_DELAY_MS * 1000) - localSkippedMicroUTCTime;
	}

	private static void startDebugTimer() {
		Thread thread = new Thread(() -> {
			try {
				long startMillis = System.currentTimeMillis();

				long nanoPassed = 0;
				long masterLostTime = 0;

				long lastTime = System.nanoTime();
				long version = 1;

				while (true) {
					Thread.sleep(1);

					long currentTime = System.nanoTime();
					long nanoDelta = currentTime - lastTime;
					long currentMicro = normalTimeMicro();

					// allow for small scheduling hick-ups to avoid loosing time
					// but limit the hick-up to filter out debug pauses
					nanoPassed += Math.min(MAX_TIMER_DELAY_MS * 1000 * 1000, nanoDelta);
					masterLostTime += Math.max(0, nanoDelta - MAX_TIMER_DELAY_MS * 1000 * 1000);

					debugTime = startMillis + nanoPassed / 1000 / 1000;
					// use versions to allow detection whether matching skippedNanoTime and lastNanoTime were read
					// if the read pre- and postVersion don't match, something may have changed in the mean time
					// update postVersion first as the thread running nanoTime must read the new value if it is still
					// inside the value copy loop
					postVersion = version;
					skippedNanoTime = masterLostTime;
					lastNanoTime = currentTime;
					skippedMicroUTCTime = masterLostTime / 1000;
					lastMicroUTCTime = currentMicro;
					preVersion = version;

					lastTime = currentTime;
					version++;
				}
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		});
		thread.setName("debug-time");
		thread.setDaemon(true);
		thread.start();
	}

	public static void sleep(long millis) throws InterruptedException {
		if (millis < 0) throw new IllegalArgumentException("Got negative sleep duration");
		long timeForWakeup = currentTimeMillis.getAsLong() + millis;
		while (true) {
			long currentTime = REFITTime.currentTimeMillis.getAsLong();
			if (currentTime >= timeForWakeup) return;

			// Sleep again if waking up too early
			Thread.sleep(timeForWakeup - currentTime);
		}
	}
}
