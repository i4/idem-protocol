package refit.util;

import java.math.BigInteger;
import java.nio.ByteBuffer;

public class REFITBigIntHelper {
	public static void putBigInt(ByteBuffer buffer, BigInteger bigint) {
		byte[] bytes = bigint.toByteArray();
		buffer.putShort((short) bytes.length);
		buffer.put(bytes);
	}

	public static BigInteger getBigInt(ByteBuffer buffer) {
		short len = buffer.getShort();
		byte[] bytes = new byte[len];
		buffer.get(bytes);
		return new BigInteger(bytes);
	}

	public static int getLen(BigInteger bigint) {
		return Short.BYTES + (bigint.bitLength() + 1 + (8 - 1)) / 8;
	}

}
