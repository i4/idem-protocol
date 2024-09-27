package refit.application.ycsb;

import java.nio.ByteBuffer;
import java.util.Collection;


public class REFITData {

	public REFITData(int capacity) {
		this.buffer = ByteBuffer.allocate(capacity);
		this.data = buffer.array();
		this.size = 0;
	}

	public REFITData(byte[] data) {
		this.buffer = ByteBuffer.wrap(data);
		this.data = data;
		this.size = data.length;
	}


	// ###########
	// # CONTENT #
	// ###########

	public final ByteBuffer buffer;
	public final byte[] data;
	public int size;


	@Override
	public String toString() {
		return "[" + size + "/" + data.length + "] " + buffer;
	}

	public REFITData sync() {
		buffer.position(0);
		buffer.limit(size);
		return this;
	}

	public REFITData clear() {
		buffer.clear();
		size = 0;
		return this;
	}

	public void complete() {
		size = buffer.position();
	}

	public void skip(int bytes) {
		buffer.position(buffer.position() + bytes);
	}

	public void get(byte[] destination) {
		buffer.get(destination);
	}

	public void copy(REFITData source) {
		for (int i = 0; i < source.size; i++) data[i] = source.data[i];
		size = source.size;
	}


	// ###########
	// # NUMBERS #
	// ###########

	public void putByte(byte value) {
		buffer.put(value);
	}

	public byte getByte() {
		return buffer.get();
	}

	public void putInt(int value) {
		buffer.putInt(value);
	}

	public int getInt() {
		return buffer.getInt();
	}

	public void putLong(long value) {
		buffer.putLong(value);
	}

	public long getLong() {
		return buffer.getLong();
	}


	// ###########
	// # STRINGS #
	// ###########

	public void putString(String string) {
		buffer.putInt(string.length());
		buffer.put(string.getBytes());
	}

	public String getString() {
		int length = buffer.getInt();
		String string = new String(data, buffer.position(), length);
		buffer.position(buffer.position() + length);
		return string;
	}

	public void putStrings(Collection<String> strings) {
		buffer.putInt(strings.size());
		for (String string : strings) putString(string);
	}

	public Collection<String> getStrings(Collection<String> strings) {
		int count = buffer.getInt();
		for (int i = 0; i < count; i++) {
			String string = getString();
			strings.add(string);
		}
		return strings;
	}


	// #################
	// # SERIALIZATION #
	// #################

	public static int size(int capacity) {
		return Integer.BYTES + capacity;
	}

	public int size() {
		return Integer.BYTES + size;
	}

	public void serialize(ByteBuffer buffer) {
		buffer.putInt(size);
		buffer.put(data, 0, size);
	}

	public void deserialize(ByteBuffer buffer) {
		size = buffer.getInt();
		buffer.get(data, 0, size);
	}

	public void skip(ByteBuffer buffer) {
		size = buffer.getInt();
		buffer.position(buffer.position() + size);
	}


	// ##############
	// # COMPARISON #
	// ##############

	@Override
	public int hashCode() {
		int code = 31 + size;
		for (int i = 0; i < size; i++) code = 31 * code + data[i];
		return code;
	}

	@Override
	public boolean equals(Object object) {
		REFITData other = (REFITData) object;
		if (size != other.size) return false;
		for (int i = 0; i < size; i++) {
			if (data[i] != other.data[i]) return false;
		}
		return true;
	}

	public static ByteBuffer toByteBuffer(REFITData data) {
		ByteBuffer bb = ByteBuffer.allocate(data.size());
		data.serialize(bb);
		bb.flip();
		return bb;
	}

	public static REFITData fromByteBuffer(ByteBuffer bb) {
		REFITData data = new REFITData(bb.remaining() - Integer.BYTES);
		data.deserialize(bb);
		return data;
	}

}
