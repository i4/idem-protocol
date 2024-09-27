package refit.application.log;

import java.nio.ByteBuffer;

import refit.message.REFITBaseMessage;


public class REFITLogReply extends REFITBaseMessage {

	public final byte[][] data;
	public boolean moreData;

	public REFITLogReply(byte[][] data, boolean moreData) {
		super();
		this.data = data;
		this.moreData = moreData;
	}

	public REFITLogReply() {
		super();
		this.data = null;
		this.moreData = false;
	}

	public REFITLogReply(ByteBuffer buffer) {
		super(buffer);

		moreData = (buffer.get() & 1) > 0;
		short count = buffer.getShort();
		if (count >= 0) {
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
		buffer.put((byte) (moreData ? 1 : 0));

		if (data != null) {
			buffer.putShort((short) data.length);
			for (byte[] datum : data) {
				buffer.putShort((short) datum.length);
				buffer.put(datum);
			}
		} else {
			buffer.putShort((short) -1);
		}
	}

	@Override
	protected int getHeaderSize() {
		int size = super.getHeaderSize() + Byte.BYTES + Short.BYTES;
		if (data != null) {
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
		return "LogReply [data.len=" + ((data != null) ? data.length : -1) + "]";
	}
}
