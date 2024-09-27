package refit.crypto;

import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.SecureRandom;

import refit.config.REFITConfig;
import refit.util.REFITPayload;

public class REFITProbeRiddle {

	public static final REFITAuthProviderFactory<REFITProbeRiddle> factory = REFITProbeRiddle::new;

	private final REFITMessageAuthentication mac;

	public REFITProbeRiddle(short id, REFITMessageAuthentication mac) {
		this.mac = mac;
	}

	// #####################
	// # PROOF OF ORDERING #
	// #####################

	public static final int RIDDLE_PART_BYTES = 32;

	public byte[] generateOrderRiddle() {
		SecureRandom random = new SecureRandom();
		byte[] riddle = new byte[RIDDLE_PART_BYTES];
		random.nextBytes(riddle);
		return riddle;
	}

	public byte[] calcStepKey(byte[] riddle, int step, short clientId, short replicaId) {
		byte[] message = new byte[riddle.length + Integer.BYTES];
		ByteBuffer bb = ByteBuffer.wrap(message);
		bb.put(riddle);
		bb.putInt(step);

		return mac.calculateMAC(message, clientId, replicaId);
	}

	public byte[] deriveDirectHashChain(byte[] stepKey, byte[] riddleInput) {
		MessageDigest messageDigest = mac.getMAC();
		messageDigest.update(stepKey);
		messageDigest.update(riddleInput);
		byte[] digest = messageDigest.digest();

		byte[] chainHash = new byte[digest.length + riddleInput.length - REFITPayload.HASH_SIZE];
		ByteBuffer bb = ByteBuffer.wrap(chainHash);
		bb.put(digest);
		bb.put(riddleInput, REFITPayload.HASH_SIZE, riddleInput.length - REFITPayload.HASH_SIZE);
		return chainHash;
	}

	public byte[] deriveHashChain(byte[] stepKey, byte[][] riddleParts) {
		int flagSize = -1;

		for (byte[] part : riddleParts) {
			if (part == null) {
				continue;
			}
			int myFlagSize = part.length - REFITPayload.HASH_SIZE;
			if (flagSize == -1) {
				flagSize = myFlagSize;
			} else if (flagSize != myFlagSize) {
				throw new IllegalArgumentException("Invalid riddle parts");
			}
		}
		if (flagSize == -1) {
			throw new IllegalArgumentException("Empty riddle O.o");
		}

		MessageDigest messageDigest = mac.getMAC();
		messageDigest.update(stepKey);

		byte[] placeholder = new byte[REFITPayload.HASH_SIZE + flagSize];
		for (byte[] part : riddleParts) {
			if (part == null) {
				messageDigest.update(placeholder);
			} else {
				messageDigest.update(part);
			}
		}

		byte[] flagBytes = deriveChunkFlags(riddleParts);
		messageDigest.update(flagBytes);
		byte[] digest = messageDigest.digest();

		byte[] chainHash = new byte[digest.length + riddleParts.length * flagSize + flagBytes.length];
		ByteBuffer bb = ByteBuffer.wrap(chainHash);
		bb.put(digest);
		for (byte[] part : riddleParts) {
			bb.put((part != null) ? part : placeholder, REFITPayload.HASH_SIZE, flagSize);
		}
		bb.put(flagBytes);
		return chainHash;
	}

	public byte[][] selectUsedRiddles(byte[][] riddleParts, short chunkFlags) {
		byte[][] selectedParts = new byte[riddleParts.length][];
		for (int i = 0; i < riddleParts.length; i++) {
			byte[] part = riddleParts[i];
			if ((chunkFlags & (1 << i)) == 0) {
				selectedParts[i] = null;
			} else {
				selectedParts[i] = part;
			}
		}
		return selectedParts;
	}

	private byte[] deriveChunkFlags(byte[][] riddleParts) {
		if (riddleParts.length > 16) {
			throw new IllegalArgumentException("Too many riddleParts to handle got " + riddleParts.length);
		}
		int chunkFlags = 0;
		for (int i = 0; i < riddleParts.length; i++) {
			if (riddleParts[i] != null) {
				chunkFlags |= (1 << i);
			}
		}

		byte[] flagBytes = new byte[2];
		ByteBuffer bb = ByteBuffer.wrap(flagBytes);
		bb.putShort((short) chunkFlags);
		return flagBytes;
	}

	public int countChunkFlags(short chunkFlags) {
		int count = 0;
		for (int i = 0; i < REFITConfig.TOTAL_NR_OF_REPLICAS; i++) {
			if ((chunkFlags & (1 << i)) != 0) {
				count++;
			}
		}
		return count;
	}

	public boolean isReplicaInFlags(short replicaId, short chunkFlags) {
		return (chunkFlags & (1 << replicaId)) != 0;
	}
}
