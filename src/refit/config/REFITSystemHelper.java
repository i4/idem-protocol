package refit.config;

public class REFITSystemHelper {
	public static String getenv(String key, String def) {
		String value = System.getenv(key);
		if (value == null) {
			value = def;
		}
		return value;
	}
}
