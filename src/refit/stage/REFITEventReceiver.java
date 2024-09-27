package refit.stage;

import refit.message.REFITEvent;

@FunctionalInterface
public interface REFITEventReceiver<M extends REFITEvent> {
	void tell(M message);
}
