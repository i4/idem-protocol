package refit.util;

public class REFITAssert {

	public static void assertTrue(final boolean result) {
		assertTrue(result, null);
	}

	public static void assertTrue(final boolean result, final String message) {
		if (!result) {
			throw new AssertionError(message);
		}
	}

	public static void fail() {
		fail(null);
	}

	public static void fail(final String message) {
		throw new AssertionError(message);
	}

	public static void notNull(Object obj) {
		assertTrue(obj != null);
	}
}
