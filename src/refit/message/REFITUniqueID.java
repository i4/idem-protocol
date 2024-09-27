package refit.message;

public class REFITUniqueID {

	public final short nodeID;
	public final long seqNr;

	public REFITUniqueID(short nodeID, long seqNr) {
		this.nodeID = nodeID;
		this.seqNr = seqNr;
	}


	@Override
	public boolean equals(Object object) {
		if (this == object) return true;
		if (object == null || getClass() != object.getClass()) return false;
		REFITUniqueID other = (REFITUniqueID) object;
		return nodeID == other.nodeID && seqNr == other.seqNr;
	}

	@Override
	public int hashCode() {
		return (((int) nodeID) << 24) + ((((int) seqNr) << 8) >> 8);
	}

	@Override
	public String toString() {
		return "[" + nodeID + "-" + seqNr + "]";
	}

}
