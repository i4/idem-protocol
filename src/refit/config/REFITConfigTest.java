package refit.config;

public class REFITConfigTest {
	public static void main(String[] args) {
		// It doesn't matter much which config parameter is accessed
		// It is only important to load and thereby initialize the REFITConfig class
		if (REFITConfig.ADDRESSES.length > 0) {
			System.exit(0);
		} else {
			System.err.println("Configuration without replicas/clients???");
			System.exit(1);
		}
	}
}
