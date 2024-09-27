package refit.agreement.idem.order;

import java.nio.ByteBuffer;

import refit.agreement.idem.IDEMMessage.IDEMOrderMessage;

public class IDEMWindow {
	public long start;
	public long end;

	private final IDEMElement[] values;
	private int idx_start;

	private int idx(long sqn) {
		return (idx_start + (int) (sqn-start)) % values.length;
	}

	public IDEMWindow(long start, int size) {
		this.start = start;
		this.end = start + size - 1;
		this.values = new IDEMElement[size];
		this.idx_start = 0;
	}

	public IDEMWindow(long start, long end) {
		this(start, (int)(end-start));
	}

	public IDEMWindow(ByteBuffer buffer) {
		this.start = buffer.getLong();
		this.end = buffer.getLong();
		this.idx_start = buffer.getInt();
		int len = buffer.getInt();
		this.values = new IDEMElement[len];
		for (int i=0; i<len; i++) {
			if (buffer.get() == 1) {
				values[i] = new IDEMElement(buffer);
			}
		}
	}

	public void serialize(ByteBuffer buffer) {
		buffer.putLong(start);
		buffer.putLong(end);
		buffer.putInt(idx_start);
		buffer.putInt(values.length);
		for (IDEMElement e : values) {
			if (e == null) {
				buffer.put((byte) 0);
			} else {
				buffer.put((byte) 1);
				e.serialize(buffer);
			}
		}
	}

	public int payloadSize() {
		int size = Long.BYTES + Long.BYTES + Integer.BYTES + Integer.BYTES;
		size += values.length + (values.length * IDEMElement.SIZE);
		return size;
	}

	public void move(long newStart) {
		if (newStart <= start) return;
		int diff = (int) (newStart - start);
		this.start = newStart;
		this.end = end + diff;
		for (int i=idx_start; i < idx_start + diff; i++) {
			// Remove old data
			values[i%values.length] = null;
		}
		this.idx_start = idx_start + diff;
	}

	public void add(IDEMOrderMessage value) {
		if (value.sqn < start) return;
		// Move window if no more space
		if (value.sqn > end) {
			move(value.sqn - (end-start-1));
		}
		// Add element
		int idx = idx(value.sqn);
		if (values[idx] == null || value.view > values[idx].view) {
			// No entry or from older view
			values[idx] = new IDEMElement(value.uid, value.view);
			values[idx].ack(value.from);
		} else if (value.view < values[idx].view) {
			return;
		} else {
			values[idx].ack(value.from);
		}
	}

	public IDEMElement get(long sqn) {
		if (sqn < start || sqn > end) return null;
		int idx = idx(sqn);
		return values[idx];
	}

	public void put(long sqn, IDEMElement e) {
		if (sqn < start || sqn > end) return;
		int idx = idx(sqn);
		values[idx] = e;
	}

	public boolean newOpinion(IDEMOrderMessage value) {
		if (value.sqn < start) return false;
		if (value.sqn > end) return true;

		int idx = idx(value.sqn);
		if (values[idx] == null || value.view > values[idx].view) {
			return true;
		} else if (value.view < values[idx].view) {
			return false;
		} else {
			return !values[idx].acks[value.from];
		}
	}

	public boolean match(long sqn, int thresh, short replica) {
		IDEMElement e = this.get(sqn);
		if (e == null) {
			return false;
		} else {
			return e.acks[replica] && e.acks() >= thresh;
		}
	}

	public int getMaxView() {
		int view = 0;
		for (int i = 0; i < values.length; i++) {
			if (values[i] != null && values[i].view > view) {
				view = values[i].view;
			}
		}
		return view;
	}

	public String metadata() {
		return "Window (" + start + "-" + end + ")";
	}

	@Override
	public String toString() {
		String ret = metadata() + ": ";
		long cur = start;
		for (long i = start; i <= end; i++) {
			ret += "{" + (cur++) + ":" + get(i) + "} ";
		}
		return ret;
	}
}
