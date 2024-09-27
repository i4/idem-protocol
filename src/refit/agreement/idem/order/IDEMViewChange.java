package refit.agreement.idem.order;

import java.nio.ByteBuffer;

import refit.config.REFITConfig;

public class IDEMViewChange {

	public int targetView;
	public IDEMWindow commits;

	public IDEMViewChange(int view, IDEMWindow commits) {
		this.targetView = view;
		this.commits = commits;
	}

	public IDEMViewChange(ByteBuffer buffer) {
		this.targetView = buffer.getInt();
		this.commits = new IDEMWindow(buffer);
	}

	public void serialize(ByteBuffer buffer) {
		buffer.putInt(targetView);
		commits.serialize(buffer);
	}

	public int payloadSize() {
		return Integer.BYTES + commits.payloadSize();
	}

	@Override
	public String toString() {
		return "VC[" + targetView + " " + commits.metadata() + "]";
	}

	public static class IDEMViewChanges {
		private IDEMViewChange[] vcs;
		private int targetView;
		private int voted;

		public IDEMViewChanges() {
			vcs = new IDEMViewChange[REFITConfig.TOTAL_NR_OF_REPLICAS];
			targetView = -1;
			voted = 0;
		}

		public int view() {
			return targetView;
		}

		public void add(IDEMViewChange vc, short replica) {
			if (vc.targetView < targetView) return;
			if (vcs[replica] != null) return;

			// New view; remove outdated view changes
			if (vc.targetView > targetView) {
				this.targetView = vc.targetView;
				for (int i=0; i < vcs.length; i++) {
					vcs[i] = null;
				}
				voted = 0;
			}
			vcs[replica] = vc;
			voted++;
		}

		public boolean stable() {
			return voted >= REFITConfig.FAULTS_TO_TOLERATE + 1;
		}

		public IDEMViewChange merge() {
			long to = 0;
			for (IDEMViewChange vc : vcs) {
				if (vc == null) continue;
				to = Math.max(to, vc.commits.end);
			}
			long from = to - REFITConfig.MAXIMUM_ORDER_INSTANCES_IN_PROGRESS;

			IDEMWindow window = new IDEMWindow(from, to);
			for (long i=from; i<=to; i++) {
				// Get element with the highest view for this sqn
				IDEMElement el = null;
				for (IDEMViewChange vc : vcs) {
					if (vc == null) continue;
					IDEMElement en = vc.commits.get(i);
					if (en != null && (el == null || en.view > el.view)) {
						el = en;
					}
				}
				window.put(i, el);
			}
			return new IDEMViewChange(targetView, window);
		}

	}
}
