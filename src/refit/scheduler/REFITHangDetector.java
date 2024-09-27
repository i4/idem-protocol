package refit.scheduler;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import refit.util.REFITLogger;
import refit.util.REFITTime;

public class REFITHangDetector extends Thread {

	private final ConcurrentHashMap<String, Entry> registrations = new ConcurrentHashMap<>();

	public REFITHangDetector() {
		setDaemon(true);
		setName("hang");
	}

	public Entry register(String name, long timeoutInUs) {
		Entry entry = new Entry(name, timeoutInUs);
		Entry previousEntry = registrations.putIfAbsent(name, entry);
		if (previousEntry != null) {
			throw new IllegalArgumentException("component name " + name + " is not unique");
		}
		return entry;
	}

	@Override
	public void run() {
		while (!isInterrupted()) {
			for (Entry entry : registrations.values()) {
				entry.check();
			}
			try {
				Thread.sleep(100);
			} catch (InterruptedException e) {
				e.printStackTrace();
				break;
			}
		}
	}

	public static class Entry {
		private final String name;
		private final AtomicLong lastRefreshTimeUs;
		private final long timeoutInUs;

		private Entry(String name, long timeoutInUs) {
			this.name = name;
			this.timeoutInUs = timeoutInUs;
			this.lastRefreshTimeUs = new AtomicLong(REFITTime.timeMicroUTC.getAsLong());
		}

		public void disable() {
			lastRefreshTimeUs.set(Long.MAX_VALUE - timeoutInUs);
		}

		public void refresh() {
			long checkTimeUs = check();
			lastRefreshTimeUs.set(checkTimeUs);
		}

		private long check() {
			long checkTimeUs = REFITTime.timeMicroUTC.getAsLong();
			if (checkTimeUs > lastRefreshTimeUs.get() + timeoutInUs) {
				REFITLogger.logError("HangDetector", "Detected hang of " + name + " for " + (checkTimeUs - lastRefreshTimeUs.get()) + "us");
			}
			return checkTimeUs;
		}
	}

}
