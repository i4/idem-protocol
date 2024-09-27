package refit.crypto;

import java.nio.ByteBuffer;
import java.security.*;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import refit.config.REFITConfig;
import refit.message.*;
import refit.replica.REFITBaseReplica;
import refit.util.REFITCallTracer;
import refit.util.REFITLogger;


public class REFITMessageAuthentication {

	public static final boolean[] ANY_SENDER = new boolean[0];

	private static final String MAC_NAME = "SHA-256";

	protected final short id;
	private static final ThreadLocal<MessageDigest> digests = new ThreadLocal<MessageDigest>() {
		@Override
		protected MessageDigest initialValue() {
			try {
				return MessageDigest.getInstance(MAC_NAME);
			} catch (NoSuchAlgorithmException nsae) {
				nsae.printStackTrace();
				throw new RuntimeException(nsae);
			}
		}
	};
	private final AtomicInteger digestCount;
	private final KeyPair signatureKeyPair;
	private final Map<Short, PublicKey> publicKeys;
	private final Map<Long, MessageDigest> keyCache = new ConcurrentHashMap<>();

	@SuppressWarnings("rawtypes")
	private final Map<Class<? extends REFITAuthProviderFactory>, Object> providers = new ConcurrentHashMap<>();
	private final Map<Class<?>, Boolean> providerClasses = new ConcurrentHashMap<>();

	public REFITMessageAuthentication(short id) {
		this.id = id;
		this.digestCount = new AtomicInteger(0);

		if (!REFITConfig.SIGNED_REQUESTS && id >= REFITConfig.TOTAL_NR_OF_REPLICAS || !REFITConfig.AUTHENTICATE_MESSAGES) {
			signatureKeyPair = null;
			publicKeys = null;
			return;
		}

		KeyPair[] keys;
		if (REFITConfig.USE_ED25519) {
			keys = REFITKeyManager.loadKey(REFITKeyManager.KeyType.ED25519, 256);
		} else {
			keys = REFITKeyManager.loadKey(REFITKeyManager.KeyType.RSA, 8 * SIGNATURE_SIZE);
		}

		signatureKeyPair = keys[id];
		publicKeys = new HashMap<>();
		for (int i = 0; i < keys.length; i++) {
			addPublicKey((short) i, keys[i].getPublic());
		}
	}

	// Do not directly call using AuthProvider::new as parameter, but store the lambda in a static variable
	// and use that everywhere
	public <T> T get(REFITAuthProviderFactory<T> factory) {
		//noinspection unchecked
		return (T) providers.computeIfAbsent(factory.getClass(), cls -> {
			T fac = factory.createProvider(id, this);
			if (REFITConfig.ENABLE_DEBUG_CHECKS && providerClasses.put(fac.getClass(), true) != null) {
				throw new AssertionError("Tried to create multiple instances of the same crypto provider! " + fac.getClass());
			}
			return fac;
		});
	}

	@Override
	public String toString() {
		return "MAUTH";
	}

	MessageDigest getMAC() {
		return digests.get();
	}

	public int processedDigestCount() {
		return digestCount.getAndSet(0);
	}

	protected boolean verifySender(REFITMessage message, boolean[] legitimateSenders) {
		if (legitimateSenders == ANY_SENDER) {
			return true;
		}
		if (message.from < 0 || message.from >= legitimateSenders.length || !legitimateSenders[message.from]) {
			REFITLogger.logWarning(this, "received message from unexpected sender " + message);
			return false;
		}
		return true;
	}

	protected void assertAuthenticatonType(REFITMessage.AuthenticationType expectedAuthType, REFITMessage message) {
		if (!message.getAuthenticationType().equals(expectedAuthType))
			throw new AssertionError("Message expects different authentication type");
	}

	protected void assertInternalSource(REFITMessage message) {
		if (message.from != id)
			throw new AssertionError("Won't authenticate message from replica " + message.from);
		if (!message.isInternal())
			throw new AssertionError("Won't authenticate message from possibly external source");
	}

	// ############
	// # NULL MAC #
	// ############

	public void appendNullMAC(REFITMessage message) {
		if (!message.getAuthenticationType().equals(REFITMessage.AuthenticationType.NONE))
			throw new AssertionError("Message expects different authentication type");

		// trust local messages
		message.markVerified();
	}

	public boolean verifyNullMAC(REFITMessage message) {
		if (!message.getAuthenticationType().equals(REFITMessage.AuthenticationType.NONE))
			throw new AssertionError("Message expects different authentication type");
		if (message.isVerified()) return true;

		message.markVerified();
		return true;
	}


	// ###############
	// # UNICAST MAC #
	// ###############

	public static final short UNICAST_MAC_SIZE = REFITConfig.AUTHENTICATE_MESSAGES ? (short) 32 : 0;


	public void appendUnicastMAC(REFITMessage message, short recipientID) {
		assertAuthenticatonType(REFITMessage.AuthenticationType.UNICAST_MAC, message);
		assertInternalSource(message);
		if (REFITConfig.TRACE_AUTH) REFITCallTracer.instance.trackStack("appendUnicastMAC");
		if (!REFITConfig.AUTHENTICATE_MESSAGES) {
			// trust local messages
			message.markVerified();
			return;
		}

		byte[] individualMAC = calculateMAC(message.getHash(), id, recipientID);

		// Append MAC
		ByteBuffer messageBuffer = message.getBuffer();
		messageBuffer.position(message.getMessageSize() - UNICAST_MAC_SIZE);
		messageBuffer.put(individualMAC);

		// trust local messages
		message.markVerified();
	}

	public boolean verifyUnicastMAC(REFITMessage message, boolean[] legitimateSenders) {
		return verifySender(message, legitimateSenders) && verifyUnicastMAC(message);
	}

	private boolean verifyUnicastMAC(REFITMessage message) {
		assertAuthenticatonType(REFITMessage.AuthenticationType.UNICAST_MAC, message);
		if (message.isVerified()) return true;
		if (REFITConfig.TRACE_AUTH) REFITCallTracer.instance.trackStack("verifyUnicastMAC");
		if (!REFITConfig.AUTHENTICATE_MESSAGES) {
			message.markVerified();
			return true;
		}

		byte[] individualMAC = calculateMAC(message.getHash(), message.from, id);

		// Compare MACs
		ByteBuffer messageBuffer = message.getBuffer();
		try {
			messageBuffer.position(message.getMessageSize() - UNICAST_MAC_SIZE);
		} catch (IllegalArgumentException e) {
			REFITLogger.logWarning(this, "invalid mac size " + message);
			return false;
		}
		// constant time comparison
		int isDifferent = 0;
		for (byte macByte : individualMAC) {
			isDifferent |= macByte ^ messageBuffer.get();
		}
		if (isDifferent != 0) {
			REFITLogger.logWarning(this, "individual message verification failed " + message);
			return false;
		}
		message.markVerified();
		return true;
	}


	// #################
	// # MULTICAST MAC #
	// #################

	// A multicast signature consists of the MACs for the selected replicas followed by an index specifying
	// the offset of each MAC
	public static final short MULTICAST_MAC_SIZE = (short) (((REFITConfig.AUTHENTICATE_MESSAGES) ? (UNICAST_MAC_SIZE + 1) : 0)
			* REFITConfig.TOTAL_NR_OF_REPLICAS);

	public static int calculateMACSize(int count) {
		return REFITConfig.TOTAL_NR_OF_REPLICAS + UNICAST_MAC_SIZE * count;
	}

	public void appendMulticastMAC(REFITMessage message) {
		appendMulticastMAC(message, REFITBaseReplica.ALL_REPLICAS);
	}

	public void appendMulticastMAC(REFITMessage message, boolean[] replicas) {
		assertAuthenticatonType(REFITMessage.AuthenticationType.MAC_AUTHENTICATOR, message);
		assertInternalSource(message);
		if (REFITConfig.TRACE_AUTH) REFITCallTracer.instance.trackStack("appendMulticastMAC");
		if (!REFITConfig.AUTHENTICATE_MESSAGES) {
			// trust local messages
			message.markVerified();
			return;
		}

		int replicaCount = 0;
		for (boolean replica : replicas) if (replica) replicaCount++;

		ByteBuffer messageBuffer = message.getPaddingBuffer();

		if (messageBuffer.remaining() != calculateMACSize(replicaCount)) {
			throw new AssertionError("Expected padding size differs from calculatedMACSize");
		}

		// Calculate and append individual MACs
		for (byte recipientID = 0; recipientID < replicas.length; recipientID++) {
			// skip replicas which don't get a MAC
			if (!replicas[recipientID]) continue;

			byte[] individualMAC = calculateMAC(message.getHash(), id, recipientID);
			messageBuffer.put(individualMAC);
		}
		// Append index
		int offsetCounter = replicaCount;
		for (boolean replica : replicas) {
			byte offset;
			if (replica) {
				offset = (byte) offsetCounter;
				offsetCounter -= 1;
			} else {
				// values <= 0 are invalid
				offset = 0;
			}
			messageBuffer.put(offset);
		}

		// trust local messages
		message.markVerified();
	}

	public boolean verifyMulticastMAC(REFITMessage message, boolean[] legitimateSenders) {
		return verifySender(message, legitimateSenders) && verifyMulticastMAC(message);
	}

	private boolean verifyMulticastMAC(REFITMessage message) {
		assertAuthenticatonType(REFITMessage.AuthenticationType.MAC_AUTHENTICATOR, message);
		if (message.isVerified()) return true;
		if (REFITConfig.TRACE_AUTH) REFITCallTracer.instance.trackStack("verifyMulticastMAC");
		if (!REFITConfig.AUTHENTICATE_MESSAGES) {
			message.markVerified();
			return true;
		}

		byte[] individualMAC = calculateMAC(message.getHash(), message.from, id);

		// Compare MACs
		ByteBuffer messageBuffer = message.getBuffer();
		try {
			// Get MAC offset
			int offset = messageBuffer.get(message.getMessageSize() - calculateMACSize(0) + id);
			if (offset <= 0 || offset > REFITConfig.TOTAL_NR_OF_REPLICAS) {
				REFITLogger.logWarning(this, "invalid mac offset " + message);
				return false;
			}
			messageBuffer.position(message.getMessageSize() - calculateMACSize(offset));
		} catch (IllegalArgumentException | IndexOutOfBoundsException e) {
			REFITLogger.logWarning(this, "invalid mac offset/size " + message);
			return false;
		}
		// constant time comparison
		int isDifferent = 0;
		for (byte macByte : individualMAC) {
			isDifferent |= macByte ^ messageBuffer.get();
		}
		if (isDifferent != 0) {
			REFITLogger.logWarning(this, "message verification failed " + message);
			return false;
		}
		message.markVerified();
		return true;
	}

	public boolean verifyMulticastMACRequest(REFITMessage request) {
		if (request.type == REFITMessageType.REQUEST_NO_OP) return true;
		if (request.type == REFITMessageType.BATCH) {
			for (REFITRequest msg : ((REFITBatch) request).requests) {
				if (msg.type == REFITMessageType.REQUEST_NO_OP) continue;
				if (!verifyMulticastMAC(msg)) {
					return false;
				}
			}
			return true;
		} else return verifyMulticastMAC(request);
	}

	private MessageDigest cachedKeyDigest(short from, short to, byte mod) {
		long idx = ((long) mod << 32) + ((long) to << 16) + ((long) from);

		try {
			if (!keyCache.containsKey(idx)) {
				MessageDigest messageDigest = (MessageDigest) getMAC().clone();
				byte[] key = new byte[64];
				key[0] = (byte) (from ^ mod);
				key[1] = (byte) (to ^ mod);
				for (int i = 2; i < key.length; i++) {
					key[i] = mod;
				}
				messageDigest.update(key);
				keyCache.put(idx, messageDigest);
			}
			return ((MessageDigest) keyCache.get(idx).clone());
		} catch (CloneNotSupportedException e) {
			throw new InternalError(e);
		}
	}

	byte[] calculateMAC(byte[] messageHash, short from, short to) {
		MessageDigest messageDigest = cachedKeyDigest(from, to, (byte) 0x36);
		messageDigest.update(messageHash);

		byte[] intermediateHash = messageDigest.digest();
		messageDigest = cachedKeyDigest(from, to, (byte) 0x5c);
		messageDigest.update(intermediateHash);

		return messageDigest.digest();
	}


	// ###########################
	// # PUBLIC-KEY CRYPTOGRAPHY #
	// ###########################

	public static final short SIGNATURE_SIZE = (REFITConfig.AUTHENTICATE_MESSAGES) ? (short) ((REFITConfig.USE_ED25519) ? 64 : REFITConfig.RSA_SIGNATURE_SIZE / 8) : 0;

	public PublicKey getPublicKey() {
		return signatureKeyPair != null ? signatureKeyPair.getPublic() : null;
	}

	public void addPublicKey(short from, PublicKey key) {
		if (publicKeys.containsKey(from) && publicKeys.get(from).equals(key)) {
			// ignore duplicate keys
			return;
		}
		if (publicKeys.containsKey(from) || from >= REFITConfig.TOTAL_NR_OF_REPLICAS + REFITConfig.TOTAL_NR_OF_CLIENTS || id < 0) {
			REFITLogger.logWarning(this, "Already got key from " + from + " or invalid id");
		}

		publicKeys.put(from, key);
	}

	public void appendSignature(REFITMessage message) {
		assertAuthenticatonType(REFITMessage.AuthenticationType.SIGNATURE, message);
		assertInternalSource(message);
		if (REFITConfig.TRACE_AUTH) REFITCallTracer.instance.trackStack("appendSignature");
		if (!REFITConfig.AUTHENTICATE_MESSAGES) {
			// trust local messages
			message.markVerified();
			return;
		}

		byte[] signatureBytes = calculateSignature(message.getHash());
		ByteBuffer messageBuffer = message.getBuffer();
		messageBuffer.position(message.getMessageSize() - SIGNATURE_SIZE);
		messageBuffer.put(signatureBytes);
		// FIXME use getPadding instead

		// trust local messages
		message.markVerified();
	}

	byte[] calculateSignature(byte[] hash) {
		byte[] signatureBytes;
		try {
			Signature signature = getSignature();
			signature.initSign(signatureKeyPair.getPrivate());
			signature.update(hash);
			signature.update((byte) id);
			signatureBytes = signature.sign();
		} catch (NoSuchAlgorithmException | InvalidKeyException | SignatureException | NoSuchProviderException e) {
			e.printStackTrace();
			System.exit(1);
			// make Java happy
			return null;
		}
		return signatureBytes;
	}

	public boolean verifySignature(REFITMessage message, boolean[] legitimateSenders) {
		return verifySender(message, legitimateSenders) && verifySignature(message);
	}

	protected boolean verifySignature(REFITMessage message) {
		assertAuthenticatonType(REFITMessage.AuthenticationType.SIGNATURE, message);
		if (message.isVerified()) return true;
		if (REFITConfig.TRACE_AUTH) REFITCallTracer.instance.trackStack("verifySignature");
		if (!REFITConfig.AUTHENTICATE_MESSAGES) {
			message.markVerified();
			return true;
		}

		byte[] signatureBytes = new byte[SIGNATURE_SIZE];
		ByteBuffer messageBuffer = message.getBuffer();
		messageBuffer.position(message.getMessageSize() - SIGNATURE_SIZE);
		messageBuffer.get(signatureBytes);

		if (checkSignature(message.getHash(), signatureBytes, message.from)) {
			message.markVerified();
			return true;
		} else {
			REFITLogger.logWarning(this, "signature verification failed " + message);
			return false;
		}
	}

	boolean checkSignature(byte[] hash, byte[] signatureBytes, short from) {
		if (!publicKeys.containsKey(from)) {
			REFITLogger.logWarning(this, "Failed to find key for server " + from);
			return false;
		}
		PublicKey pubKey = publicKeys.get(from);

		try {
			Signature signature = getSignature();
			signature.initVerify(pubKey);
			signature.update(hash);
			signature.update((byte) from);
			return signature.verify(signatureBytes);
		} catch (NoSuchAlgorithmException | InvalidKeyException | SignatureException | NoSuchProviderException e) {
			e.printStackTrace();
			System.exit(1);
			return false;
		}
	}

	private Signature getSignature() throws NoSuchAlgorithmException, NoSuchProviderException {
		if (REFITConfig.USE_ED25519) {
			return Signature.getInstance("NONEwithEdDSA", "EdDSA");
		} else {
			return Signature.getInstance("SHA256withRSA");
		}
	}

	public boolean verifySignatureRequest(REFITOrderRequest request) {
		if (request.type == REFITMessageType.REQUEST_NO_OP) return true;
		if (request.type == REFITMessageType.BATCH) {
			for (REFITRequest msg : ((REFITBatch) request).requests) {
				if (msg.type == REFITMessageType.REQUEST_NO_OP) continue;
				if (!verifySignature(msg)) {
					return false;
				}
			}
			return true;
		} else return verifySignature(request);
	}
}
