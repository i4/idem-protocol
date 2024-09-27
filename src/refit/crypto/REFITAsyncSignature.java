package refit.crypto;

import refit.message.REFITEvent;
import refit.message.REFITMessage;
import refit.message.REFITMessageType;
import refit.message.REFITOrderRequest;
import refit.stage.REFITBoundedReplicaContext;
import refit.stage.REFITEventReceiver;

public class REFITAsyncSignature {

	public static final REFITAuthProviderFactory<REFITAsyncSignature> factory = REFITAsyncSignature::new;

	private REFITSignatureProcessingStage asyncStage;
	private final REFITMessageAuthentication mac;

	public REFITAsyncSignature(short id, REFITMessageAuthentication mac) {
		this.mac = mac;
	}

	public enum AsyncResult {
		INVALID,
		VALID,
		PENDING,
	}

	public void setupSignatureProcessingStage(REFITBoundedReplicaContext replica, int threads) {
		if (asyncStage != null) throw new IllegalStateException("already initialized");
		asyncStage = new REFITSignatureProcessingStage(replica, threads);
	}

	public <T extends REFITMessage> void appendSignatureAsync(T message, REFITEventReceiver<? super T> callback) {
		if (asyncStage == null) {
			mac.appendSignature(message);
			callback.tell(message);
			return;
		}
//		if (REFITConfig.TRACE_AUTH) REFITCallTracer.instance.trackStack("appendSignatureAsync");

		//noinspection unchecked
		asyncStage.tell(new REFITSignatureProcessingStage.SignMessage(message, (REFITEventReceiver<? super REFITMessage>) callback));
	}

	// callback method must be thread-safe, it should only "tell" a stage about the processed message
	public <T extends REFITMessage> AsyncResult verifySignatureAsync(T message, boolean[] legitimateSenders,
	                                                                 REFITEventReceiver<? super T> callback) {
		if (asyncStage == null) {
			return mac.verifySignature(message, legitimateSenders) ? AsyncResult.VALID : AsyncResult.INVALID;
		}
		if (message.isVerified()) return AsyncResult.VALID;
//		if (REFITConfig.TRACE_AUTH) REFITCallTracer.instance.trackStack("verifySignatureAsync");

		//noinspection unchecked
		asyncStage.tell(new REFITSignatureProcessingStage.VerifyMessage(message, legitimateSenders, (REFITEventReceiver<? super REFITMessage>) callback));

		return AsyncResult.PENDING;
	}

	// callback method must be thread-safe, it should only "tell" a stage about the processed message
	public AsyncResult verifySignatureRequestAsync(REFITOrderRequest request, REFITEventReceiver<REFITEvent> callback, REFITEvent signal) {
		if (asyncStage == null) {
			return mac.verifySignatureRequest(request) ? AsyncResult.VALID : AsyncResult.INVALID;
		}
		if (request.type == REFITMessageType.REQUEST_NO_OP)
			return AsyncResult.VALID;
		if (request.isVerified()) return AsyncResult.VALID;

		asyncStage.tell(new REFITSignatureProcessingStage.VerifyRequest(request, callback, signal));

		return AsyncResult.PENDING;
	}
}
