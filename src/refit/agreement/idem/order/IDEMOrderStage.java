package refit.agreement.idem.order;

import refit.agreement.idem.IDEMEvent.IDEMCheckpointEvent;
import refit.agreement.idem.IDEMEvent.IDEMViewEvent;
import refit.agreement.idem.IDEMEvent.IDEMViewInit;
import refit.agreement.idem.IDEMMessage.IDEMCheckpointRequest;
import refit.agreement.idem.IDEMMessage.IDEMCommit;
import refit.agreement.idem.IDEMMessage.IDEMPropose;
import refit.agreement.idem.IDEMMessage.IDEMRequire;
import refit.agreement.idem.IDEMMessage.IDEMViewChangeMessage;

import java.util.ArrayList;
import java.util.List;

import refit.agreement.idem.IDEMRequestSet;
import refit.agreement.idem.IDEMStage;
import refit.agreement.idem.execution.IDEMCheckpoint;
import refit.agreement.idem.order.IDEMViewChange.IDEMViewChanges;
import refit.config.REFITConfig;
import refit.message.REFITEvent;
import refit.replica.REFITBaseReplica;
import refit.scheduler.REFITSchedulerTaskType;
import refit.scheduler.REFITTimer;
import refit.stage.REFITObserver;
import refit.stage.REFITReplicaContext;
import refit.util.REFITLogger;

public class IDEMOrderStage extends IDEMStage {

	private enum Mode {
		NORMAL,
		IDLE,
		VIEW_CHANGE
	}

	// Proposer Stage
	private Mode mode;
	private long sqn;
	private int view;
	private final IDEMRequestSet<IDEMElement> requests;
	private long[] proposed;

	private final IDEMViewChanges viewchanges;
	private IDEMViewChange merged;
	private IDEMCheckpoint checkpoint;
	private REFITTimer vcTimer;

	// Committer Stage
	private final IDEMWindow proposals;

	// Senders
	public final REFITObserver<IDEMCommit> commitSender;
	public final REFITObserver<IDEMViewEvent> viewSender;

	public IDEMOrderStage(REFITReplicaContext replica, IDEMCheckpoint initialCheckpoint) {
		super(REFITSchedulerTaskType.ORDER_STAGE, replica);

		this.mode = REFITBaseReplica.isPrimary(view, replica.id) ? Mode.NORMAL : Mode.IDLE;
		this.sqn = 0;
		this.view = 0;
		this.requests = new IDEMRequestSet<IDEMElement>();
		this.proposed = new long[REFITConfig.TOTAL_NR_OF_CLIENTS];
		this.viewchanges = new IDEMViewChanges();
		this.merged = null;
		this.checkpoint = initialCheckpoint;
		this.proposals = new IDEMWindow(0, REFITConfig.MAXIMUM_ORDER_INSTANCES_IN_PROGRESS);

		this.commitSender = new REFITObserver<>();
		this.viewSender = new REFITObserver<>();
	}

	@Override
	public void init() {
		super.init();
		// vcTimer?!
	}

	@Override
	protected void handleMessage(REFITEvent message) {
		if (message.getClass() == IDEMRequire.class) {
			handleRequire((IDEMRequire) message);
		} else if (message.getClass() == IDEMViewChangeMessage.class) {
			handleViewChange((IDEMViewChangeMessage) message);
		} else if (message.getClass() == IDEMPropose.class) {
			handleProposal((IDEMPropose) message);
		} else if (message.getClass() == IDEMCheckpointEvent.class) {
			// Receive new checkpoint from execution stage
			updateCheckpoint(((IDEMCheckpointEvent) message).checkpoint);
		} else if (message.getClass() == IDEMViewInit.class) {
			// Pass view change timer after initialization
			vcTimer = ((IDEMViewInit) message).timer;
		} else if (message.getClass() == IDEMViewEvent.class) {
			// Receive new view from execution stage
			IDEMViewEvent ve = (IDEMViewEvent) message;
			updateView(ve.view, ve.viewchange ? Mode.VIEW_CHANGE : Mode.NORMAL);
		} else {
			REFITLogger.logError(this, "drop message of unexpected type " + message.getClass().getCanonicalName());
		}
	}

	private void handleRequire(IDEMRequire r) {
		if (mode == Mode.IDLE) return;
		if (r.uid.seqNr < proposed[r.uid.nodeID]) return;

		IDEMElement el = requests.get(r.uid);
		if (el == null) {
			el = new IDEMElement(r.uid);
			requests.put(r.uid, el);
		}
		el.ack(r.from);
		if (mode == Mode.NORMAL && el.acks() >= REFITConfig.FAULTS_TO_TOLERATE + 1) {
			propose(el);
		}
	}

	private void handleViewChange(IDEMViewChangeMessage vcm) {
		IDEMViewChange vc = vcm.viewchange;
		if (vc.targetView < view) return;
		// Store view change
		viewchanges.add(vc, vcm.from);

		// If enough view change messages, update view and start view change process
		if (viewchanges.stable()) {
			updateView(viewchanges.view(), Mode.VIEW_CHANGE);
			vcTimer.startIfNotRunning(REFITConfig.REPLICA_REQUEST_TIMEOUT); // TODO: Different timeout here?

			// If current proposer: Determine which requests to resend
			if (mode == Mode.VIEW_CHANGE) {
				merged = viewchanges.merge();
				// Query checkpoint if replica outdated
				if (checkpoint.sqn < merged.commits.start) {
					tellOthers(new IDEMCheckpointRequest(merged.commits.start, replica.id));
				} else {
					completeViewChange();
				}
			}
		}
	}

	private void propose(IDEMElement e) {
		propose(e, this.sqn);
	}

	private void propose(IDEMElement el, long n) {
		IDEMPropose p = new IDEMPropose(el.id, n, view, replica.id);
		if (REFITLogger.LOG_ORDER) REFITLogger.logOrder(this, "Proposing " + p.uid + " for request " + el.id);
		tellAll(p);
		this.sqn = n + 1;

		// Update proposed and empty request set
		proposed[el.id.nodeID] = Math.max(proposed[el.id.nodeID], el.id.seqNr + 1);
		requests.remove(el.id);
	}

	private void completeViewChange() {
		// Update own state and re-propose requests
		this.sqn = checkpoint.sqn;
		this.proposed = checkpoint.executed;
		for (long n = checkpoint.sqn; n < merged.commits.end; n++) {
			IDEMElement el = merged.commits.get(n);
			if (el == null) break;
			propose(el, n);
		}

		// Propose already acked requests
		List<IDEMElement> repropose = new ArrayList<>();
		for (IDEMElement e : requests.values()) {
			if (e.acks() >= REFITConfig.FAULTS_TO_TOLERATE + 1) {
				repropose.add(e);
			}
		}
		for (IDEMElement e : repropose) {
			propose(e);
		}
		this.mode = Mode.NORMAL;
	}

	private void handleProposal(IDEMPropose p) {
		if (p.view < view) return;
		if (!proposals.newOpinion(p)) return;

		// If from a higher view, update own view
		if (p.view > view) {
			updateView(p.view, Mode.NORMAL);
		}
		// Store proposal and forward to executor
		proposals.add(p);
		IDEMCommit c = new IDEMCommit(p, replica.id);
		if (!REFITLogger.LOG_ORDER) REFITLogger.logOrder(this, "Sending commit " + c.uid);
		tellAll(c, commitSender);
		// Count propose as commit from the leader
		if (p.from != replica.id) {
			IDEMCommit cl = new IDEMCommit(p, p.from);
			commitSender.tell(cl);
		}
	}

	private void updateView(int view, Mode mode) {
		if (view <= this.view) return;

		// Update state depending on new view
		if (!REFITBaseReplica.isPrimary(view, replica.id)) {
			// Reset requests when aborting VIEW_CHANGE Mode
			if (this.mode == Mode.VIEW_CHANGE) {
				requests.clear();
			}
			this.mode = Mode.IDLE;
		} else {
			this.mode = mode;
		}

		// Reset view change timer
		vcTimer.clear();

		// Send view change messages if entering view change
		if (mode == Mode.VIEW_CHANGE) {
			IDEMViewChange vc = new IDEMViewChange(view, proposals);
			tellAll(new IDEMViewChangeMessage(vc, replica.id));
		}
		// Update view
		this.view = view;
		this.viewSender.tell(new IDEMViewEvent(view, false));
	}

	private void updateCheckpoint(IDEMCheckpoint checkpoint) {
		if (checkpoint.sqn <= this.checkpoint.sqn) return;
		this.checkpoint = checkpoint;

		// Trying to complete view change if currently in progress
		if (mode != Mode.VIEW_CHANGE) return;
		if (merged == null || merged.targetView != view) return;
		if (checkpoint.sqn < merged.commits.start) return;

		completeViewChange();
	}
}
