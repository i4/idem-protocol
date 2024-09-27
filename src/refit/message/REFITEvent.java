package refit.message;

public interface REFITEvent {
	// Subclasses must be either abstract OR final
	// Base for internal events / instructions and REFITMessages which can be sent between nodes

	StackTraceElement[] getSte();

	default Class<? extends REFITEvent> groupType() {
		return this.getClass();
	}
}
