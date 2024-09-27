package refit.agreement.idem.execution;

import java.nio.ByteBuffer;

import refit.application.REFITResult;
import refit.message.REFITRequest.RequestMode;
import refit.message.REFITUniqueID;

public class IDEMResult {

	public REFITUniqueID uid;
	public REFITResult result;
	public long sqn;
	public int view;
	public RequestMode mode;

	public IDEMResult(REFITUniqueID uid, REFITResult result, long sqn, int view, RequestMode mode) {
		this.uid = uid;
		this.result = result;
		this.sqn = sqn;
		this.view = view;
		this.mode = mode;
	}

	public IDEMResult(ByteBuffer buffer) {
		this.uid = new REFITUniqueID(buffer.getShort(), buffer.getLong());
		this.sqn = buffer.getLong();
		this.view = buffer.getInt();
		this.mode = RequestMode.values()[buffer.getInt()];
		int bufferSize = buffer.getInt();
		ByteBuffer resultBuffer = buffer.slice();
		resultBuffer.limit(bufferSize);
		this.result = new REFITResult(resultBuffer);
		buffer.position(buffer.position() + bufferSize);
	}

	public void serialize(ByteBuffer buffer) {
		buffer.putShort(uid.nodeID);
		buffer.putLong(uid.seqNr);
		buffer.putLong(sqn);
		buffer.putInt(view);
		buffer.putInt(mode.ordinal());
		ByteBuffer reply = result.getReply();
		buffer.putInt(reply.remaining());
		buffer.put(reply);
	}

	public int payloadSize() {
		return Short.BYTES + 2*Long.BYTES + 3*Integer.BYTES + result.getReply().remaining();
	}

	@Override
	public String toString() {
		return String.format("{%s|%d|%d|%s|%d bytes}", uid, sqn, view, mode, result.getReply().remaining()) ;
	}
}
