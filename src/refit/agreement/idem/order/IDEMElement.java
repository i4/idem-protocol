package refit.agreement.idem.order;

import java.nio.ByteBuffer;

import refit.config.REFITConfig;
import refit.message.REFITUniqueID;

public class IDEMElement {
	public static final int SIZE = Short.BYTES + Long.BYTES + Long.BYTES;

	public REFITUniqueID id;
	public int view;
	protected transient boolean[] acks;

	public IDEMElement(REFITUniqueID id) {
		this(id, -1);
	}

	public IDEMElement(REFITUniqueID id, int view) {
		this.id = id;
		this.view = view;
		this.acks = new boolean[REFITConfig.TOTAL_NR_OF_REPLICAS];
	}

	public IDEMElement(ByteBuffer buffer) {
		this.id = new REFITUniqueID(buffer.getShort(), buffer.getLong());
		this.view = buffer.getInt();
		this.acks = new boolean[REFITConfig.TOTAL_NR_OF_REPLICAS];
	}

	public void ack(short replica) {
		acks[replica] = true;
	}

	public int acks() {
		int count = 0;
		for (int i=0; i<acks.length; i++) {
			if (acks[i]) count++;
		}
		return count;
	}

	public void serialize(ByteBuffer buffer) {
		buffer.putShort(id.nodeID);
		buffer.putLong(id.seqNr);
		buffer.putInt(view);
	}

	@Override
	public String toString() {
		return "E[" + id + "|" + view + "]";
	}
}
