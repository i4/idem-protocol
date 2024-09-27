package refit.stage;

import java.util.Arrays;

import refit.message.REFITEvent;

public class REFITObserver<M extends REFITEvent> implements REFITEventReceiver<M> {
	private REFITEventReceiver<? super M>[] listeners;

	public REFITObserver() {
		//noinspection unchecked
		this.listeners = (REFITEventReceiver<? super M>[]) new REFITEventReceiver[0];
	}

	@Override
	public void tell(M message) {
		//noinspection ForLoopReplaceableByForEach
		for (int i = 0; i < listeners.length; i++) {
			listeners[i].tell(message);
		}
	}

	public void addListener(REFITEventReceiver<? super M> stage) {
		if (stage != null) {
			for (REFITEventReceiver<? super M> registeredStage : listeners) {
				if (registeredStage == stage) {
					throw new AssertionError("A stage must only be registered once for each listener");
				}
			}
			// accept a listener which can handle M or a super type
			listeners = addStage(listeners, stage);
		}
	}

	public void addListenersFrom(REFITObserver<? super M> observer) {
		for (REFITEventReceiver<? super M> listener : observer.listeners) {
			addListener(listener);
		}
	}

	private REFITEventReceiver<? super M>[] addStage(REFITEventReceiver<? super M>[] stages, REFITEventReceiver<? super M> stage) {
		stages = Arrays.copyOf(stages, stages.length + 1);
		stages[stages.length - 1] = stage;
		return stages;
	}
}
