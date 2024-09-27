package refit.application.ycsb;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.atomic.AtomicInteger;

import refit.client.REFITClientLibrary;
import refit.client.REFITRejectException;
import refit.config.REFITConfig;
import refit.message.REFITRequest.RequestMode;
import refit.util.REFITTime;
import refit.util.REFITIntervalStatistics.ResultType;
import site.ycsb.ByteIterator;
import site.ycsb.DB;
import site.ycsb.DBException;
import site.ycsb.Status;

public class REFITYCSBClient extends DB {

	private static final boolean VERBOSE = false;
	private static final AtomicInteger CLIENT_IDS = new AtomicInteger(0);

	private final REFITClientLibrary service;
	private Status status;


	public REFITYCSBClient() {
		// Initialize attributes
		this.service = REFITYCSBBenchmark.ycsb[CLIENT_IDS.getAndIncrement()];
		this.status = Status.SERVICE_UNAVAILABLE;
	}

	@Override
	public void init() throws DBException {
		super.init();
		// read one value to make sure the client is ready
		read("usertable", "user6284781860667377211", null, new HashMap<>());
	}

	// ############
	// # REQUESTS #
	// ############

	@Override
	public Status read(String table, String key, Set<String> fields, Map<String, ByteIterator> result) {
		if (VERBOSE) System.out.println("[Client - Read] " + table + " " + key + " " + fields + " " + result);

		// Marshal request
		REFITData buffer = new REFITData(REFITYCSBBenchmark.REQUEST_MAX);
		buffer.putByte(REFITYCSBOperation.READ.index);
		buffer.putString(table);
		buffer.putString(key);
		if (fields == null) {
			buffer.putByte((byte) 0);
		} else {
			buffer.putByte((byte) 1);
			buffer.putStrings(fields);
		}
		buffer.complete();

		// Invoke operation
		REFITData reply = invoke(buffer, true);
		if (status.isOk()) REFITYCSBDatabase.getRecord(reply, result, false);
		return status;
	}

	@Override
	public Status scan(String table, String startkey, int recordcount, Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
		if (VERBOSE)
			System.out.println("[Client - Scan] " + table + " " + startkey + " " + recordcount + " " + fields + " " + result);

		// Marshal request
		REFITData buffer = new REFITData(REFITYCSBBenchmark.REQUEST_MAX);
		buffer.putByte(REFITYCSBOperation.SCAN.index);
		buffer.putString(table);
		buffer.putString(startkey);
		buffer.putInt(recordcount);
		if (fields == null) {
			buffer.putByte((byte) 0);
		} else {
			buffer.putByte((byte) 1);
			buffer.putStrings(fields);
		}
		buffer.complete();

		// Invoke operation
		REFITData reply = invoke(buffer, true);

		// Check status
		if (!status.isOk()) return status;

		// Unmarshal records
		int size = reply.getInt();
		for (int i = 0; i < size; i++) {
			HashMap<String, ByteIterator> values = new HashMap<String, ByteIterator>();
			REFITYCSBDatabase.getRecord(reply, values, false);
			result.add(values);
		}
		return status;
	}

	@Override
	public Status update(String table, String key, Map<String, ByteIterator> values) {
		// Marshal request
		if (VERBOSE) System.out.println("[Client - Update] " + table + " " + key + " " + values.keySet());
		REFITData buffer = new REFITData(REFITYCSBBenchmark.REQUEST_MAX);
		buffer.putByte(REFITYCSBOperation.UPDATE.index);
		buffer.putString(table);
		buffer.putString(key);
		REFITYCSBDatabase.putRecord(buffer, values);
		buffer.complete();

		// Invoke operation
		invoke(buffer, false);
		return status;
	}

	@Override
	public Status insert(String table, String key, Map<String, ByteIterator> values) {
		// Marshal request
		if (VERBOSE) System.out.println("[Client - Insert] " + table + " " + key + " " + values.keySet());
		REFITData buffer = new REFITData(REFITYCSBBenchmark.REQUEST_MAX);
		buffer.putByte(REFITYCSBOperation.INSERT.index);
		buffer.putString(table);
		buffer.putString(key);
		REFITYCSBDatabase.putRecord(buffer, values);
		buffer.complete();

		// Invoke operation
		invoke(buffer, false);
		return status;
	}

	@Override
	public Status delete(String table, String key) {
		// Marshal request
		if (VERBOSE) System.out.println("[Client - Delete] " + table + " " + key);
		REFITData buffer = new REFITData(REFITYCSBBenchmark.REQUEST_MAX);
		buffer.putByte(REFITYCSBOperation.DELETE.index);
		buffer.putString(table);
		buffer.putString(key);
		buffer.complete();

		// Invoke operation
		invoke(buffer, false);
		return status;
	}

	private REFITData invoke(REFITData request, boolean isRead) {
		boolean reject = false;
		REFITData result = null;

		do {
			// Invoke operation
			long startTime = REFITTime.nanoTime.getAsLong();
			try {
				ByteBuffer resultBB = service.invoke(REFITData.toByteBuffer(request), isRead ? RequestMode.READ : RequestMode.WRITE);
				result = REFITData.fromByteBuffer(resultBB);
				status = REFITYCSBStatus.STATUSES[result.getByte()].status;
				long endTime = REFITTime.nanoTime.getAsLong();
				service.getStatistics().event((long) ((endTime - startTime) / 1000f), service.getNodeID(), (isRead) ? "r" : "w", service.resultAfterReject ? ResultType.ACCEPT_REJECT : ResultType.ACCEPT);
				reject = false;
			} catch (Exception e) {
				// e.printStackTrace();
				if (e.getClass() == REFITRejectException.class) {
					ResultType rt = ((REFITRejectException) e).full ? ResultType.FULL_REJECT : ResultType.REJECT_TIMEOUT;
					reject = true;
					// Rejection
					long endTime = REFITTime.nanoTime.getAsLong();
					if (REFITConfig.CLIENT_SPLIT_REJECTS) {
						service.getRejectStatistics().event((long) ((endTime - startTime) / 1000f), service.getNodeID(), "reject", rt);
					} else {
						service.getStatistics().event((long) ((endTime - startTime) / 1000f), service.getNodeID(), "reject", rt);
					}
					// After reject, wait a semi random timeout before trying next request
					try {
						int timeout =  (int) ((Math.random() + 1) * (REFITConfig.REJECT_MAX_IDLE/2));
						Thread.sleep(timeout);
					} catch (InterruptedException e2) {}
				} else {
					e.printStackTrace();
					status = Status.UNEXPECTED_STATE;
					break;
				}
			}
		} while (reject);
		return result;
	}
}
