package refit.application.ycsb;

import java.util.*;
import java.util.Map.Entry;

import site.ycsb.*;


public class REFITYCSBDatabase extends DB {

	private static final boolean VERBOSE = false;
	// allow the ycsb server to retrieve the static instance and get a checkpoint from it
	static REFITYCSBDatabase staticInstance;
	static boolean dontCleanup = false;


	// #########
	// # TABLE #
	// #########

	private static class REFITYCSBTable extends TreeMap<String, Map<String, ByteIterator>> {

		public void store(REFITData destination) {
			// Store table size
			destination.putInt(size());

			// Store records
			for (Entry<String, Map<String, ByteIterator>> record : entrySet()) {
				destination.putString(record.getKey());
				putRecord(destination, record.getValue());
			}
		}

		public void load(REFITData source, boolean copy) {
			// Retrieve table size
			int size = source.getInt();

			// Retrieve records
			for (int i = 0; i < size; i++) {
				String key = source.getString();
				Map<String, ByteIterator> record = getRecord(source, new HashMap<String, ByteIterator>(), copy);
				put(key, record);
			}
		}

	}


	// ############
	// # DATABASE #
	// ############

	private final Map<String, REFITYCSBTable> database;


	public REFITYCSBDatabase() {
		this.database = new TreeMap<String, REFITYCSBTable>();
		staticInstance = this;
	}


	@Override
	public void init() throws DBException {
		// Create user table
		database.put("usertable", new REFITYCSBTable());

//		// Insert record
//		Map<String, ByteIterator> values = new HashMap<String, ByteIterator>();
//		values.put("field0", new ByteArrayByteIterator("user6284781860667377211:field0:-56807877:2032869390:-165488160:1762371712:-169193395:-1039977118:-10".getBytes()));
//		values.put("field1", new ByteArrayByteIterator("user6284781860667377211:field1:-56807846:-2133018101:933062878:139614226:1263461560:1124615364:-3349".getBytes()));
//		values.put("field2", new ByteArrayByteIterator("user6284781860667377211:field2:-56807815:-2003938296:1922731651:495642034:-69297697:-1304257243:1357".getBytes()));
//		values.put("field3", new ByteArrayByteIterator("user6284781860667377211:field3:-56807784:-1874878672:1967282817:-1787301858:-652319809:-382740913:-6".getBytes()));
//		values.put("field4", new ByteArrayByteIterator("user6284781860667377211:field4:-56807753:-1745798867:119712592:1203333019:-1789354410:478574555:-484".getBytes()));
//		values.put("field5", new ByteArrayByteIterator("user6284781860667377211:field5:-56807722:-1616719062:-1126019200:-413767825:-1320471713:-916924287:7".getBytes()));
//		values.put("field6", new ByteArrayByteIterator("user6284781860667377211:field6:-56807691:-1487659438:-913284994:1592021384:432601397:-1261814014:-18".getBytes()));
//		values.put("field7", new ByteArrayByteIterator("user6284781860667377211:field7:-56807660:-1358579633:1534112728:-525774:1901187587:681272202:-151847".getBytes()));
//		values.put("field8", new ByteArrayByteIterator("user6284781860667377211:field8:-56807629:-1229500479:-1960212955:-831317430:-395506705:-155385368:44".getBytes()));
//		values.put("field9", new ByteArrayByteIterator("user6284781860667377211:field9:-56807598:-1100440855:1189962510:701512652:-166179663:2032990533:2915".getBytes()));
//		insert("usertable", "user6284781860667377211", values);
	}

	@Override
	public void cleanup() throws DBException {
		if (dontCleanup) return;
		database.clear();
	}

	@Override
	public Status read(String table, String key, Set<String> fields, Map<String, ByteIterator> result) {
		// Get table
		if (VERBOSE) System.out.println("[Database - Read] " + table + " " + key + " " + fields + " " + result);
		REFITYCSBTable t = database.get(table);
		if (t == null) return Status.NOT_FOUND;

		// Get record
		Map<String, ByteIterator> record = t.get(key);
		if (record == null) return Status.NOT_FOUND;

		// Retrieve fields
		retrieve(record, fields, result);
		return Status.OK;
	}

	@Override
	public Status scan(String table, String startkey, int recordcount, Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
		// Check record count
		if (VERBOSE)
			System.out.println("[Database - Scan] " + table + " " + startkey + " " + recordcount + " " + fields + " " + result);
		if (recordcount <= 0) return Status.OK;

		// Get table
		REFITYCSBTable t = database.get(table);
		if (t == null) return Status.NOT_FOUND;

		// Check whether the start record exists
		if (!t.containsKey(startkey)) return Status.ERROR;

		// Retrieve records
		for (Entry<String, Map<String, ByteIterator>> entry : t.tailMap(startkey).entrySet()) {
			Map<String, ByteIterator> record = t.get(entry.getKey());
			HashMap<String, ByteIterator> values = new HashMap<String, ByteIterator>();
			retrieve(record, fields, values);
			result.add(values);
			if (--recordcount <= 0) break;
		}
		return Status.OK;
	}

	@Override
	public Status update(String table, String key, Map<String, ByteIterator> values) {
		// Get table
		if (VERBOSE) System.out.println("[Database - Update] " + table + " " + key + " " + values.keySet());
		REFITYCSBTable t = database.get(table);
		if (t == null) return Status.NOT_FOUND;

		// Get record
		Map<String, ByteIterator> record = t.get(key);
		if (record == null) return Status.NOT_FOUND;

		// Update record
		record.putAll(values);
		return Status.OK;
	}

	@Override
	public Status insert(String table, String key, Map<String, ByteIterator> values) {
		// Get table
		if (VERBOSE) System.out.println("[Database - Insert] " + table + " " + key + " " + values.keySet());
		REFITYCSBTable t = database.get(table);
		if (t == null) return Status.NOT_FOUND;

		// Check whether the record already exists
		if (t.containsKey(key)) return Status.ERROR;

		// Insert record
		t.put(key, values);
		return Status.OK;
	}

	@Override
	public Status delete(String table, String key) {
		// Get table
		if (VERBOSE) System.out.println("[Database - Delete] " + table + " " + key);
		REFITYCSBTable t = database.get(table);
		if (t == null) return Status.NOT_FOUND;

		// Delete record
		Map<String, ByteIterator> record = t.remove(key);
		return (record != null) ? Status.OK : Status.NOT_FOUND;
	}

	public void store(REFITData destination) {
		destination.putInt(database.size());
		for (Entry<String, REFITYCSBTable> table : database.entrySet()) {
			destination.putString(table.getKey());
			table.getValue().store(destination);
		}
		destination.complete();
	}

	public void load(REFITData source, boolean copy) {
		int size = source.getInt();
		for (int i = 0; i < size; i++) {
			String key = source.getString();
			REFITYCSBTable table = new REFITYCSBTable();
			table.load(source, copy);
			database.put(key, table);
		}
	}


	// ###########
	// # HELPERS #
	// ###########

	private static void retrieve(Map<String, ByteIterator> record, Set<String> fields, Map<String, ByteIterator> result) {
		if (fields != null) {
			// Retrieve specific fields
			for (String field : fields) {
				ByteIterator value = record.get(field);
				if (value == null) continue;
				value.reset();
				result.put(field, value);
			}
			return;
		} else {
			// Retrieve all fields
			for (Entry<String, ByteIterator> entry : record.entrySet()) {
				ByteIterator value = entry.getValue();
				value.reset();
				result.put(entry.getKey(), value);
			}
		}
	}

	public static void putRecord(REFITData buffer, Map<String, ByteIterator> record) {
		try {
			// Store record size
			buffer.putInt(record.size());

			// Store fields
			for (Entry<String, ByteIterator> field : record.entrySet()) {
				// Store key
				buffer.putString(field.getKey());

				// Store value
				ByteIterator value = field.getValue();
				buffer.putInt((int) value.bytesLeft());
				while (value.hasNext()) buffer.putByte(value.nextByte());
			}
		} catch (Exception e) {
			System.err.println(e + ": " + buffer + " " + record.size());
		}
	}

	public static Map<String, ByteIterator> getRecord(REFITData buffer, Map<String, ByteIterator> record, boolean copy) {
		// Retrieve record size
		int size = buffer.getInt();

		// Retrieve fields
		for (int i = 0; i < size; i++) {
			// Retrieve key
			String key = buffer.getString();

			// Retrieve value
			int length = buffer.getInt();
			ByteArrayByteIterator value;
			if (copy) {
				byte[] array = new byte[length];
				buffer.get(array);
				value = new ByteArrayByteIterator(array);
			} else {
				value = new ByteArrayByteIterator(buffer.data, buffer.buffer.position(), length);
				buffer.skip(length);
			}

			// Store field
			record.put(key, value);
		}
		return record;
	}

}
