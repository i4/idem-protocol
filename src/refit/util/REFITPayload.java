package refit.util;

import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;


public class REFITPayload {

	public static final int HASH_SIZE = 32;
	private static final String HASH_FUNCTION = "SHA-256";

	// No intended for instantiation
	private REFITPayload() {
	}

	// #########################
	// # STATIC HELPER METHODS #
	// #########################

	private static final ThreadLocal<MessageDigest> digests = ThreadLocal.withInitial(() -> {
		try {
			return MessageDigest.getInstance(HASH_FUNCTION);
		} catch (final NoSuchAlgorithmException nsae) {
			nsae.printStackTrace();
			throw new RuntimeException(nsae);
		}
	});

	public static MessageDigest getDigest() {
		return digests.get();
	}

	public static void pauseableUpdate(MessageDigest digest, ByteBuffer buffer) {
		// feed small blocks to digest to enforce a few JVM safepoints, which are required to stop
		// all threads during GC
		final int block = 512 * 1024;
		if (buffer.remaining() > block) {
			int orig = buffer.limit();
			for (int i = buffer.position() + block; i < orig; i += block) {
				buffer.limit(i);
				digest.update(buffer);
			}
			if (buffer.limit() < orig) {
				buffer.limit(orig);
				digest.update(buffer);
			}
		} else {
			digest.update(buffer);
		}
	}

	public static String toString(byte[] data) {
		StringBuilder s = new StringBuilder(2 * data.length);
		for (byte b : data) s.append(String.format("%02x", b));
		return s.toString();
	}

	// helper function for "evaluate expression" in the debugger
	public static String bbToString(ByteBuffer bb) {
		int pos = bb.position();
		byte[] bytes = new byte[bb.remaining()];
		bb.get(bytes);
		bb.position(pos);
		// return string as that way simplifies copying the full content of a message in intellij
		return Arrays.toString(bytes);
	}

	public static byte[] bbToBytes(ByteBuffer bb) {
		int pos = bb.position();
		byte[] bytes = new byte[bb.remaining()];
		bb.get(bytes);
		bb.position(pos);
		return bytes;
	}

}
