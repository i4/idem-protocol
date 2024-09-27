package refit.application.ycsb;

import java.util.HashMap;
import java.util.Map;

import site.ycsb.Status;

public enum REFITYCSBStatus {

	OK(Status.OK),
	BATCHED_OK(Status.BATCHED_OK),
	ERROR(Status.ERROR),
	NOT_FOUND(Status.NOT_FOUND);

	public static final REFITYCSBStatus[] STATUSES = values();
	public static final Map<Status, REFITYCSBStatus> MAP = new HashMap<Status, REFITYCSBStatus>();

	static {
		for (int i = 0; i < REFITYCSBStatus.STATUSES.length; i++)
			MAP.put(REFITYCSBStatus.STATUSES[i].status, REFITYCSBStatus.STATUSES[i]);
	}

	public final byte index;
	public final Status status;


	REFITYCSBStatus(Status status) {
		this.index = (byte) ordinal();
		this.status = status;
	}


	public static byte index(Status status) {
		REFITYCSBStatus s = MAP.get(status);
		return s.index;
	}

}
