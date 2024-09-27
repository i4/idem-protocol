package refit.application.ycsb;

import java.nio.ByteBuffer;
import java.util.*;

import refit.application.REFITApplicationServer;
import refit.application.REFITResult;
import refit.message.REFITUniqueID;
import refit.replica.checkpoint.REFITCheckpointObject;
import site.ycsb.ByteIterator;
import site.ycsb.Client;
import site.ycsb.DBException;
import site.ycsb.Status;

public class REFITYCSBServer implements REFITApplicationServer {

	private REFITYCSBDatabase database;


	public REFITYCSBServer() {
	}

	@Override
	public String toString() {
		return "YCSB";
	}

	private static REFITCheckpointObject[] dbSeed;

	synchronized public static void generateDB() {
		// only execute once
		if (dbSeed != null) return;

		REFITYCSBDatabase.dontCleanup = true;
		String[] args = new String[]{
				"-load",
				"-threads", String.valueOf(1),
				"-P", "workloads/workload" + REFITYCSBBenchmark.WORKLOAD,
				"-db", REFITYCSBDatabase.class.getName(),
				"-p", "dataintegrity=true",
				"-s"
		};
		Client.main(args);

		REFITData data = new REFITData(REFITYCSBBenchmark.STATE_MAX);
		REFITYCSBDatabase.staticInstance.store(data);

		// cleanup
		REFITYCSBDatabase.staticInstance = null;
		REFITYCSBDatabase.dontCleanup = false;

		ByteBuffer bb = REFITData.toByteBuffer(data);
		REFITCheckpointObject checkpointObject = new REFITCheckpointObject(bb.array());
		dbSeed = new REFITCheckpointObject[]{checkpointObject};

//		MessageDigest digest = REFITPayload.getDigest();
//		REFITPayload.pauseableUpdate(digest, bb);
//		REFITLogger.logWarning("datahash", REFITPayload.toString(digest.digest()));
	}

	public void init() {
		generateDB();
		database = new REFITYCSBDatabase();
		try {
			database.init();
		} catch (DBException e) {
			throw new RuntimeException(e);
		}
		applyCheckpoint(dbSeed);
	}

	@Override
	public boolean checkReadOnly(ByteBuffer payload) {
		REFITYCSBOperation o = REFITYCSBOperation.OPERATIONS[payload.get(0)];
		return REFITYCSBOperation.READ == o || REFITYCSBOperation.SCAN == o;
	}

	private final REFITData static_result = new REFITData(REFITYCSBBenchmark.RESULT_MAX);

	@Override
	public REFITResult processRequest(REFITUniqueID uid, long agreementSeqNr, ByteBuffer payload) {
		static_result.clear();

		REFITData operation = REFITData.fromByteBuffer(payload);
		REFITYCSBOperation o = REFITYCSBOperation.OPERATIONS[operation.getByte()];
		switch (o) {
		case READ:
			read(operation, static_result);
			break;
		case SCAN:
			scan(operation, static_result);
			break;
		case UPDATE:
			update(operation, static_result);
			break;
		case INSERT:
			insert(operation, static_result);
			break;
		case DELETE:
			delete(operation, static_result);
			break;
		default:
			throw new RuntimeException("Unexpected operation: " + o);
		}

		return new REFITResult(REFITData.toByteBuffer(static_result));
	}

	private void read(REFITData request, REFITData response) {
		// Unmarshal request
		String table = request.getString();
		String key = request.getString();
		Set<String> fields = (request.getByte() == 0) ? null : (Set<String>) request.getStrings(new HashSet<String>());

		// Execute operation
		Map<String, ByteIterator> values = new HashMap<String, ByteIterator>();
		Status status = database.read(table, key, fields, values);

		// Marshal response
		response.putByte(REFITYCSBStatus.index(status));
		if (status.isOk()) REFITYCSBDatabase.putRecord(response, values);
		response.complete();
	}

	private void scan(REFITData request, REFITData response) {
		// Unmarshal request
		String table = request.getString();
		String startkey = request.getString();
		int recordcount = request.getInt();
		Set<String> fields = (request.getByte() == 0) ? null : (Set<String>) request.getStrings(new HashSet<String>());

		// Execute operation
		Vector<HashMap<String, ByteIterator>> result = new Vector<HashMap<String, ByteIterator>>();
		Status status = database.scan(table, startkey, recordcount, fields, result);

		// Marshal response
		response.putByte(REFITYCSBStatus.index(status));
		if (status.isOk()) {
			response.putInt(result.size());
			for (HashMap<String, ByteIterator> record : result) REFITYCSBDatabase.putRecord(response, record);
		}
		response.complete();
	}

	private void update(REFITData request, REFITData response) {
		// Unmarshal request
		String table = request.getString();
		String key = request.getString();
		Map<String, ByteIterator> values = REFITYCSBDatabase.getRecord(request, new HashMap<String, ByteIterator>(), true);

		// Execute operation
		Status status = database.update(table, key, values);

		// Marshal response
		response.putByte(REFITYCSBStatus.index(status));
		response.complete();
	}

	private void insert(REFITData request, REFITData response) {
		// Unmarshal request
		String table = request.getString();
		String key = request.getString();
		Map<String, ByteIterator> values = REFITYCSBDatabase.getRecord(request, new HashMap<String, ByteIterator>(), true);

		// Execute operation
		Status status = database.insert(table, key, values);

		// Marshal response
		response.putByte(REFITYCSBStatus.index(status));
		response.complete();
	}

	private void delete(REFITData request, REFITData response) {
		// Unmarshal request
		String table = request.getString();
		String key = request.getString();

		// Execute operation
		Status status = database.delete(table, key);

		// Marshal response
		response.putByte(REFITYCSBStatus.index(status));
		response.complete();
	}


	private final REFITData static_data = new REFITData(REFITYCSBBenchmark.STATE_MAX);

	@Override
	public REFITCheckpointObject[] createCheckpoint() {
		static_data.clear();
		database.store(static_data);
		ByteBuffer bb = REFITData.toByteBuffer(static_data);
		REFITCheckpointObject checkpointObject = new REFITCheckpointObject(bb.array());
		return new REFITCheckpointObject[]{checkpointObject};
	}

	@Override
	public void applyCheckpoint(final REFITCheckpointObject[] checkpoint) {
		if (checkpoint.length != 1) throw new InternalError("Broken checkpoint");
		REFITData data = REFITData.fromByteBuffer(checkpoint[0].getBuffer());
		database.load(data, true);
	}
}
