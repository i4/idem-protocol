package refit.application.log;

import java.nio.ByteBuffer;

import refit.message.REFITBaseMessage;


public class REFITLogRequest extends REFITBaseMessage {

	enum LogOperation {
		APPEND,
		READ_RANGE,
		GARBAGE_COLLECT
	}

	public final LogOperation operation;
	public final long start;
	public final short count;
	public final byte[] data;

	public REFITLogRequest(byte[] data) {
		super();
		this.operation = LogOperation.APPEND;
		this.start = -1;
		this.count = -1;
		this.data = data;
	}

	public REFITLogRequest(long start, short count) {
		super();
		this.operation = LogOperation.READ_RANGE;
		this.start = start;
		this.count = count;
		this.data = null;
	}

	public REFITLogRequest(long start) {
		super();
		this.operation = LogOperation.GARBAGE_COLLECT;
		this.start = start;
		this.count = -1;
		this.data = null;
	}

	public REFITLogRequest(ByteBuffer buffer) {
		super(buffer);

		operation = LogOperation.values()[buffer.get()];
		switch (operation) {
		case APPEND:
			short len = buffer.getShort();
			data = new byte[len];
			buffer.get(data);
			start = -1;
			count = -1;
			break;
		case READ_RANGE:
			start = buffer.getLong();
			count = buffer.getShort();
			data = null;
			break;
		case GARBAGE_COLLECT:
			start = buffer.getLong();
			count = -1;
			data = null;
			break;
		default:
			throw new IllegalArgumentException("Invalid operation type " + operation);
		}
	}

	@Override
	protected void serialize(ByteBuffer buffer) {
		super.serialize(buffer);

		buffer.put(((byte) operation.ordinal()));
		switch (operation) {
		case APPEND:
			buffer.putShort((short) data.length);
			buffer.put(data);
			break;
		case READ_RANGE:
			buffer.putLong(start);
			buffer.putShort(count);
			break;
		case GARBAGE_COLLECT:
			buffer.putLong(start);
			break;
		default:
			throw new IllegalArgumentException("Invalid operation type " + operation);
		}
	}

	@Override
	protected int getHeaderSize() {
		int size = super.getHeaderSize() + Byte.BYTES;
		switch (operation) {
		case APPEND:
			size += Short.BYTES + data.length;
			break;
		case READ_RANGE:
			size += Long.BYTES + Short.BYTES;
			break;
		case GARBAGE_COLLECT:
			size += Long.BYTES;
			break;
		}
		return size;
	}

	@Override
	protected int calculatePayloadSize() {
		return getHeaderSize();
	}

	@Override
	public String toString() {
		return "LogRequest [operation=" + operation + ", start= " + start + ", count=" + count + ", data.len=" + ((data != null) ? data.length : -1) + "]";
	}
}
