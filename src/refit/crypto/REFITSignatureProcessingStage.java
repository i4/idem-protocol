package refit.crypto;

import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import refit.message.*;
import refit.scheduler.REFITSchedulerTaskType;
import refit.stage.REFITBoundedReplicaContext;
import refit.stage.REFITBoundedStage;
import refit.stage.REFITEventReceiver;
import refit.util.REFITLogger;

public class REFITSignatureProcessingStage extends REFITBoundedStage {
	public static final class SignMessage extends REFITEventClass {
		public final REFITMessage request;
		public final REFITEventReceiver<? super REFITMessage> callback;

		public SignMessage(REFITMessage request, REFITEventReceiver<? super REFITMessage> callback) {
			this.request = request;
			this.callback = callback;
		}
	}

	public static final class VerifyMessage extends REFITEventClass {
		public final REFITMessage request;
		public final boolean[] legitimateSenders;
		public final REFITEventReceiver<? super REFITMessage> callback;

		public VerifyMessage(REFITMessage request, boolean[] legitimateSenders, REFITEventReceiver<? super REFITMessage> callback) {
			this.request = request;
			this.legitimateSenders = legitimateSenders;
			this.callback = callback;
		}
	}

	public static final class VerifyRequest extends REFITEventClass {
		public final REFITOrderRequest request;
		public final REFITEventReceiver<REFITEvent> callback;
		private final REFITEvent signal;

		public VerifyRequest(REFITOrderRequest request, REFITEventReceiver<REFITEvent> callback, REFITEvent signal) {
			this.request = request;
			this.callback = callback;
			this.signal = signal;
		}
	}

	private final Queue<SignMessage> signQueue;
	private final Queue<VerifyMessage> verifyQueue;
	private final Queue<VerifyRequest> verifyRequestQueue;
	private final Object waiter;

	public REFITSignatureProcessingStage(REFITBoundedReplicaContext replica, int threads) {
		super(REFITSchedulerTaskType.SIGNATURE_STAGE, replica);

		if (threads > 0) {
			if (replica.group.isSingleThreaded) {
				throw new AssertionError("SPS with threads cannot be used with a single threaded scheduler");
			}
			signQueue = new ConcurrentLinkedQueue<>();
			verifyQueue = new ConcurrentLinkedQueue<>();
			verifyRequestQueue = new ConcurrentLinkedQueue<>();
			waiter = new Object();
			for (int i = 0; i < threads; i++) {
				new Thread(() -> {
					while (true) {
						while (!signQueue.isEmpty() || !verifyQueue.isEmpty() || !verifyRequestQueue.isEmpty()) {
							processOneSignature();
						}
						synchronized (waiter) {
							try {
								waiter.wait();
							} catch (InterruptedException e) {
								break;
							}
						}
					}
				}).start();
			}
		} else {
			signQueue = new LinkedList<>();
			verifyQueue = new LinkedList<>();
			verifyRequestQueue = new LinkedList<>();
			waiter = null;
		}
	}

	@Override
	public String toString() {
		return "SPS";
	}

	@Override
	protected void handleMessage(REFITEvent message) {
		if (message.getClass() == SignMessage.class) {
			signQueue.add(((SignMessage) message));
		} else if (message.getClass() == VerifyMessage.class) {
			verifyQueue.add(((VerifyMessage) message));
		} else if (message.getClass() == VerifyRequest.class) {
			verifyRequestQueue.add(((VerifyRequest) message));
		} else {
			REFITLogger.logError(this, "drop message of unexpected type " + message.getClass().getCanonicalName());
		}
		if (waiter != null) {
			synchronized (waiter) {
				waiter.notify();
			}
		}
	}

	private void processOneSignature() {
		// Prefer sign requests
		SignMessage sm = signQueue.poll();
		if (sm != null) {
			replica.mac.appendSignature(sm.request);
			sm.callback.tell(sm.request);
			return;
		}

		VerifyRequest vr = verifyRequestQueue.poll();
		if (vr != null) {
			REFITOrderRequest request = vr.request;
			boolean isValid = replica.mac.verifySignatureRequest(request);
			if (isValid && request.type == REFITMessageType.BATCH) {
				// mark whole batch as valid
				request.markVerified();
			}

			if (isValid) {
				vr.callback.tell(vr.signal);
			} else {
				REFITLogger.logWarning(this, "dropping request with invalid signature: " + request);
			}
			return;
		}

		VerifyMessage vm = verifyQueue.poll();
		if (vm != null) {
			boolean isValid = replica.mac.verifySignature(vm.request, vm.legitimateSenders);
			if (isValid) {
				vm.callback.tell(vm.request);
			} else {
				REFITLogger.logWarning(this, "dropping message with invalid signature: " + vm.request);
			}
		}
	}

	@Override
	public void stageComplete() {
		if (waiter != null) {
			return;
		}

		processOneSignature();
		if (!signQueue.isEmpty() || !verifyQueue.isEmpty() || !verifyRequestQueue.isEmpty()) {
			// request further executions
			progress();
		}
	}
}
