package refit.agreement.idem;

import java.nio.ByteBuffer;

import refit.agreement.idem.execution.IDEMCheckpoint;
import refit.agreement.idem.order.IDEMViewChange;
import refit.message.REFITMessage;
import refit.message.REFITMessageType;
import refit.message.REFITReplyBase;
import refit.message.REFITUniqueID;

public abstract class IDEMMessage extends REFITMessage {

	protected IDEMMessage(REFITMessageType type, REFITUniqueID uid, short from) {
		super(type, uid, from);
	}

	public IDEMMessage(REFITMessageType type, ByteBuffer buffer) {
		super(type, buffer);
	}

	@Override
	public AuthenticationType getAuthenticationType() {
		return AuthenticationType.NONE;
	}

	// #####################
	// # SPECIFIC MESSAGES #
	// #####################

	public static class IDEMReject extends REFITReplyBase {
		public IDEMReject(REFITUniqueID rid, short from) {
			super(REFITMessageType.IDEM_REJECT, rid, from, from);
		}

		public IDEMReject(ByteBuffer buffer) {
			super(REFITMessageType.IDEM_REJECT, buffer);
		}

		@Override
		public byte[] getPayloadHash() {
			return new byte[0];
		}

		@Override
		public boolean isFullReply() {
			return false;
		}

		@Override
		public int viewID() {
			return -1;
		}

		@Override
		public long sequenceNumber() {
			return uid.seqNr;
		}

		@Override
		public AuthenticationType getAuthenticationType() {
			return AuthenticationType.NONE;
		}
	}

	public static class IDEMRequire extends IDEMMessage {
		public IDEMRequire(REFITUniqueID rid, short from) {
			super(REFITMessageType.IDEM_REQUIRE, rid, from);
		}

		public IDEMRequire(ByteBuffer buffer) {
			super(REFITMessageType.IDEM_REQUIRE, buffer);
		}
	}

	public static class IDEMForwardRequest extends IDEMMessage {
		public IDEMForwardRequest(REFITUniqueID rid, short from) {
			super(REFITMessageType.IDEM_FORWARD_REQUEST, rid, from);
		}

		public IDEMForwardRequest(ByteBuffer buffer) {
			super(REFITMessageType.IDEM_FORWARD_REQUEST, buffer);
		}
	}

	/* Order Messages */
	public static abstract class IDEMOrderMessage extends IDEMMessage {

		public static final int HEADER_SIZE = Long.BYTES + Long.BYTES;

		public long sqn;
		public int view;

		protected IDEMOrderMessage(REFITMessageType type, REFITUniqueID request, long sqn, int view, short from) {
			super(type, request, from);
			this.sqn = sqn;
			this.view = view;
		}

		protected IDEMOrderMessage(REFITMessageType type, ByteBuffer buffer) {
			super(type, buffer);
			this.sqn = buffer.getLong();
			this.view = buffer.getInt();
		}

		@Override
		public AuthenticationType getAuthenticationType() {
			return AuthenticationType.NONE;
		}

		@Override
		protected void serialize(ByteBuffer buffer) {
			super.serialize(buffer);
			buffer.putLong(sqn);
			buffer.putInt(view);
		}

		@Override
		public int calculateMessageSize() {
			return super.calculateMessageSize() + HEADER_SIZE;
		}

		@Override
		public String toString() {
			return super.toString() + "[" + sqn + "|" + view + "]";
		}
	}

	public static class IDEMPropose extends IDEMOrderMessage {
		public IDEMPropose(REFITUniqueID request, long sqn, int view, short from) {
			super(REFITMessageType.IDEM_PROPOSE, request, sqn, view, from);
		}

		public IDEMPropose(ByteBuffer buffer) {
			super(REFITMessageType.IDEM_PROPOSE, buffer);
		}
	}

	public static class IDEMCommit extends IDEMOrderMessage {
		public IDEMCommit(IDEMPropose p, short from) {
			super(REFITMessageType.IDEM_COMMIT, p.uid, p.sqn, p.view, from);
		}

		public IDEMCommit(ByteBuffer buffer) {
			super(REFITMessageType.IDEM_COMMIT, buffer);
		}
	}

	/* View Change */
	public static class IDEMViewChangeMessage extends IDEMMessage {

		public IDEMViewChange viewchange;

		public IDEMViewChangeMessage(IDEMViewChange viewchange, short from) {
			super(REFITMessageType.IDEM_VIEW_CHANGE, new REFITUniqueID(from, viewchange.targetView), from);
			this.viewchange = viewchange;
		}

		public IDEMViewChangeMessage(ByteBuffer buffer) {
			super(REFITMessageType.IDEM_VIEW_CHANGE, buffer);
			this.viewchange = new IDEMViewChange(buffer);
		}

		public long view() {
			return this.uid.seqNr;
		}

		@Override
		protected void serialize(ByteBuffer buffer) {
			super.serialize(buffer);
			viewchange.serialize(buffer);
		}

		@Override
		protected int calculatePayloadSize() {
			return super.calculatePayloadSize() + viewchange.payloadSize();
		}
	}

	/* Checkpointing */
	public static class IDEMCheckpointRequest extends IDEMMessage {
		public IDEMCheckpointRequest(long sqn, short from) {
			super(REFITMessageType.IDEM_CHECKPOINT_REQUEST, new REFITUniqueID(from, sqn), from);
		}

		public IDEMCheckpointRequest(ByteBuffer buffer) {
			super(REFITMessageType.IDEM_CHECKPOINT_REQUEST, buffer);
		}
	}


	public static class IDEMCheckpointMessage extends IDEMMessage {

		public IDEMCheckpoint checkpoint;

		public IDEMCheckpointMessage(IDEMCheckpoint checkpoint, short from) {
			super(REFITMessageType.IDEM_CHECKPOINT, new REFITUniqueID(from, checkpoint.sqn), from);
			this.checkpoint = checkpoint;
		}

		public IDEMCheckpointMessage(ByteBuffer buffer) {
			super(REFITMessageType.IDEM_CHECKPOINT, buffer);
			this.checkpoint = new IDEMCheckpoint(buffer);
		}

		@Override
		protected void serialize(ByteBuffer buffer) {
			super.serialize(buffer);
			checkpoint.serialize(buffer);
		}

		@Override
		protected int calculatePayloadSize() {
			return super.calculatePayloadSize() + checkpoint.payloadSize();
		}
	}
}
