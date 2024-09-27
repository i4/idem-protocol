package refit.message;

import refit.config.REFITConfig;

public abstract class REFITEventClass implements REFITEvent {
	private final StackTraceElement[] ste;

	protected REFITEventClass() {
		ste = (REFITConfig.TRACE_MESSAGES) ? Thread.currentThread().getStackTrace() : null;
	}

	public StackTraceElement[] getSte() {
		return ste;
	}
}
