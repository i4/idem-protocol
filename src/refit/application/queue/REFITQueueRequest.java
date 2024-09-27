package refit.application.queue;

import java.nio.ByteBuffer;

import refit.message.REFITBaseMessage;


public class REFITQueueRequest extends REFITBaseMessage {

	public final boolean isEnqueue;
	public final short count;
	public final byte[] data;

	public REFITQueueRequest(byte[] data) {
		super();
		this.isEnqueue = true;
		this.count = -1;
		this.data = data;
	}

	public REFITQueueRequest(short count) {
		super();
		this.isEnqueue = false;
		this.count = count;
		this.data = null;
	}

	public REFITQueueRequest(ByteBuffer buffer) {
		super(buffer);

		isEnqueue = ((buffer.get() & 1) > 0);
		if (isEnqueue) {
			short len = buffer.getShort();
			data = new byte[len];
			buffer.get(data);
			count = -1;
		} else {
			data = null;
			count = buffer.getShort();
		}
	}

	@Override
	protected void serialize(ByteBuffer buffer) {
		super.serialize(buffer);

		buffer.put((byte) (isEnqueue ? 1 : 0));
		if (isEnqueue) {
			buffer.putShort((short) data.length);
			buffer.put(data);
		} else {
			buffer.putShort(count);
		}
	}

	@Override
	protected int getHeaderSize() {
		int size = super.getHeaderSize() + Byte.BYTES + Short.BYTES;
		if (data != null) {
			size += data.length;
		}
		return size;
	}

	@Override
	protected int calculatePayloadSize() {
		return getHeaderSize();
	}

	@Override
	public String toString() {
		return "QueueRequest [isEnqueue=" + isEnqueue + ", count=" + count + ", data.len=" + ((data != null) ? data.length : -1) + "]";
	}
}
