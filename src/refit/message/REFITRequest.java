package refit.message;

import java.nio.ByteBuffer;

import refit.config.REFITConfig;
import refit.util.REFITAssert;

public final class REFITRequest extends REFITOrderRequest {

	public enum RequestMode {
		READ,
		READ_WEAK,
		WRITE,
		WRITE_ACK,
		PROBE,
		SYSTEM,
		;

		public RequestMode toStrongMode() {
			// used to upgrade READ_WEAK to READ when REFITConfig.ONLY_CONSISTENT is set
			if (this == READ_WEAK) {
				return READ;
			}
			return this;
		}

		public RequestMode toPanicMode() {
			// adapt mode if necessary for panic requests
			if (!isTotalOrder())
				return WRITE;
			if (REFITConfig.USE_PBFT_READ_OPTIMIZATION && this.equals(RequestMode.READ))
				return WRITE;
			return this;
		}

		public boolean isTotalOrder() {
			// return whether the request should yield linearizable results
			switch (this) {
			case READ:
			case WRITE:
			case WRITE_ACK:
			case SYSTEM:
				return true;
			case READ_WEAK:
			case PROBE:
				return false;
			default:
				throw new InternalError();
			}
		}

		public boolean isReadOnly() {
			// return whether the request will modify the application state or not
			switch (this) {
			case READ:
			case READ_WEAK:
				return true;
			case WRITE:
			case WRITE_ACK:
			case PROBE:
			case SYSTEM:
				return false;
			default:
				throw new InternalError();
			}
		}
	}

	public static class Builder {
		private final REFITUniqueID uid;
		private final ByteBuffer payload;
		private short from;
		private long executeAfter = -1L;
		private short replyReplicaID = -1;
		private RequestMode mode = RequestMode.WRITE;

		private Builder(REFITUniqueID uid, ByteBuffer payload) {
			this.uid = uid;
			this.from = uid.nodeID;
			this.payload = payload;
		}

		public Builder from(short from) {
			this.from = from;
			return this;
		}

		public Builder executeAfter(long executeAfter) {
			this.executeAfter = executeAfter;
			return this;
		}

		public Builder replyReplicaID(short replyReplicaID) {
			this.replyReplicaID = replyReplicaID;
			return this;
		}

		public Builder mode(RequestMode mode) {
			this.mode = mode;
			return this;
		}

		public REFITRequest build() {
			return new REFITRequest(this);
		}
	}

	public static Builder builder(REFITUniqueID uid, ByteBuffer payload) {
		return new Builder(uid, payload);
	}


	private static final int REQUEST_HEADER_SIZE = Byte.BYTES + Long.BYTES + Short.BYTES + Short.BYTES;

	public final long executeAfter;
	public boolean isPanic;
	public final RequestMode mode;
	public final short replyReplicaID;
	private ByteBuffer payload;

	public boolean isForward;
	public transient long timestamp;

	private REFITRequest(Builder builder) {
		super(REFITMessageType.REQUEST, builder.uid, builder.from);
		this.isPanic = false;
		this.mode = builder.mode;
		this.executeAfter = builder.executeAfter;
		this.replyReplicaID = builder.replyReplicaID;
		this.payload = builder.payload;
		this.isForward = false;
	}

	public REFITRequest(ByteBuffer buffer) {
		super(REFITMessageType.REQUEST, buffer);
		byte flags = buffer.get();
		this.isPanic = ((flags & 1) != 0);
		this.isForward = (((flags >> 1) & 1) != 0);
		int modeIdx = (flags >> 2) & 0xf;
		this.mode = RequestMode.values()[modeIdx];
		this.executeAfter = buffer.getLong();
		this.replyReplicaID = buffer.getShort();
		short payloadSize = buffer.getShort();
		this.payload = buffer.slice();
		this.payload.limit(payloadSize);
	}

	@Override
	public AuthenticationType getAuthenticationType() {
		if (REFITConfig.SIGNED_REQUESTS) {
			// getAuthenticationType() is called during the initialization when mode is not set yet
			if (mode == null) return AuthenticationType.NONE;
			if (mode.isTotalOrder()) {
				return AuthenticationType.SIGNATURE;
			} else {
				return AuthenticationType.NONE;
			}
		} else {
			return AuthenticationType.MAC_AUTHENTICATOR;
		}
	}

	public void markPanic() {
		isPanic = true;
	}

	public void markForward() {
		isForward = true;
	}

	public ByteBuffer getPayload() {
		return payload.slice();
	}

	@Override
	public boolean equals(Object object) {
		if (this == object) return true;
		if (object == null || getClass() != object.getClass()) return false;
		if (!super.equals(object)) return false;
		REFITRequest other = (REFITRequest) object;
		if (mode != other.mode) return false;
		return payload.equals(other.payload);
	}

	@Override
	protected void serialize(ByteBuffer buffer) {
		super.serialize(buffer);
		int modeIdx = mode.ordinal();
		REFITAssert.assertTrue(modeIdx < 16, "Only up to 16 request modes are supported");
		buffer.put((byte) ((isPanic ? 1 : 0) + ((isForward ? 1 : 0) << 1) + (modeIdx << 2)));
		buffer.putLong(executeAfter);
		buffer.putShort(replyReplicaID);
		int payloadSize = payload.remaining();
		buffer.putShort((short) payloadSize);
		// replace command ByteBuffer after serialization to store the data only once
		ByteBuffer newPayload = buffer.slice();
		buffer.put(payload);
		newPayload.limit(payloadSize);
		payload = newPayload;
	}

	@Override
	public void relocateTo(ByteBuffer target) {
		super.relocateTo(target);
		ByteBuffer buffer = getInternalBuffer();
		buffer.position(getHeaderSize());
		ByteBuffer newPayload = buffer.slice();
		newPayload.limit(payload.limit());
		payload = newPayload;
	}

	@Override
	protected int getHeaderSize() {
		return (super.getHeaderSize() + REQUEST_HEADER_SIZE);
	}

	@Override
	protected int calculatePayloadSize() {
		return (super.calculatePayloadSize() + payload.remaining());
	}
}
