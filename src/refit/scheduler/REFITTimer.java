package refit.scheduler;

import refit.util.REFITTime;

public class REFITTimer implements Comparable<REFITTimer> {
	@FunctionalInterface
	public interface TimerCallback {
		void timeout();
	}

	private final REFITSchedulerFacade scheduler;
	private final long timerID;
	private final TimerCallback callback;
	// must not change as long as the timer is active!!!
	private long timeout = -1L;

	private boolean hasExpired = false;

	// the compare functions assume the timerID to be unique
	protected REFITTimer(REFITSchedulerFacade scheduler, long timerID, TimerCallback callback) {
		this.scheduler = scheduler;
		this.timerID = timerID;
		this.callback = callback;
		clear();
	}

	@Override
	public String toString() {
		return "Timer<" + timerID + "@" + timeout + (hasExpired ? "*" : "") + '>';
	}

	public long currentTimeout() {
		return timeout;
	}

	public void startIfNotRunning(int delayInMs) {
		if (isRunning()) return;
		start(delayInMs);
	}

	public void start(int delayInMs) {
		startAbsolute(delayInMs + REFITTime.currentTimeMillis.getAsLong());
	}

	public void startAbsolute(long newTimeout) {
		// don't reregister if nothing changes
		if (isRunning() && timeout == newTimeout) return;

		// MUST stop the timer before updating the timeout
		clear();

		timeout = newTimeout;
		scheduler.queueTimer(this);
	}

	public void clear() {
		if (isRunning()) scheduler.stopTimer(this);
		timeout = -1L;
		hasExpired = false;
	}

	public boolean isRunning() {
		return timeout != -1L && !hasExpired;
	}

	public boolean hasExpired() {
		return hasExpired;
	}

	public void timeout() {
		hasExpired = true;
		if (callback != null) {
			callback.timeout();
		}
	}

	@Override
	public int compareTo(REFITTimer o) {
		if (timeout < o.timeout) return -1;
		if (timeout > o.timeout) return 1;
		return Long.compare(timerID, o.timerID);
	}
}
