package refit.application.queue;

import java.nio.ByteBuffer;

import refit.message.REFITBaseMessage;


public class REFITQueueReply extends REFITBaseMessage {

	public final boolean isEnqueue;
	public final byte[][] data;

	public REFITQueueReply(byte[][] data) {
		super();
		this.isEnqueue = false;
		this.data = data;
	}

	public REFITQueueReply() {
		super();
		this.isEnqueue = true;
		this.data = null;
	}

	public REFITQueueReply(ByteBuffer buffer) {
		super(buffer);

		isEnqueue = ((buffer.get() & 1) > 0);
		if (!isEnqueue) {
			short count = buffer.getShort();
			data = new byte[count][];
			for (int i = 0; i < count; i++) {
				short len = buffer.getShort();
				data[i] = new byte[len];
				buffer.get(data[i]);
			}
		} else {
			data = null;
		}
	}

	@Override
	protected void serialize(ByteBuffer buffer) {
		super.serialize(buffer);

		buffer.put((byte) (isEnqueue ? 1 : 0));
		if (!isEnqueue) {
			buffer.putShort((short) data.length);
			for (byte[] datum : data) {
				buffer.putShort((short) datum.length);
				buffer.put(datum);
			}
		}
	}

	@Override
	protected int getHeaderSize() {
		int size = super.getHeaderSize() + Byte.BYTES;
		if (!isEnqueue) {
			size += Short.BYTES;
			for (byte[] datum : data) {
				size += Short.BYTES + datum.length;
			}
		}
		return size;
	}

	@Override
	protected int calculatePayloadSize() {
		return getHeaderSize();
	}

	@Override
	public String toString() {
		return "QueueReply [isEnqueue=" + isEnqueue + ", data.len=" + ((data != null) ? data.length : -1) + "]";
	}
}
