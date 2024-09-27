package refit.agreement.idem;

import refit.agreement.idem.execution.IDEMCheckpoint;
import refit.message.REFITEventClass;
import refit.scheduler.REFITTimer;

public class IDEMEvent {

	public static final class IDEMCheckpointEvent extends REFITEventClass {
		public final IDEMCheckpoint checkpoint;

		public IDEMCheckpointEvent(IDEMCheckpoint checkpoint) {
			this.checkpoint = checkpoint;
		}
	}

	public static class IDEMViewEvent extends REFITEventClass {
		public final int view;
		public final boolean viewchange;

		public IDEMViewEvent(int view, boolean viewchange) {
			this.view = view;
			this.viewchange = viewchange;
		}
	}

	public static final class IDEMViewInit extends IDEMViewEvent {
		public final REFITTimer timer;

		public IDEMViewInit(REFITTimer timer) {
			super(-1,false);
			this.timer = timer;
		}
	}

}
