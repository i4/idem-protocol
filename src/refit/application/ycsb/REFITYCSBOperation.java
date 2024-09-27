package refit.application.ycsb;

public enum REFITYCSBOperation {

	READ,
	SCAN,
	UPDATE,
	INSERT,
	DELETE;

	public static final REFITYCSBOperation[] OPERATIONS = values();
	public final byte index;


	REFITYCSBOperation() {
		this.index = (byte) ordinal();
	}
}
