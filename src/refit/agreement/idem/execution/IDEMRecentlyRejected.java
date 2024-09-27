package refit.agreement.idem.execution;

import java.util.HashMap;

import refit.config.REFITConfig;
import refit.message.REFITRequest;
import refit.message.REFITUniqueID;

public class IDEMRecentlyRejected {
    
    	private final HashMap<REFITUniqueID, REFITRequest>[] rejectsPerClient;
        private final REFITRequest[] recentlyRejected;
        private int position;

        public IDEMRecentlyRejected() {
            this.rejectsPerClient = new HashMap[REFITConfig.TOTAL_NR_OF_CLIENTS];
            for (int i = 0; i < rejectsPerClient.length; i++) {
                this.rejectsPerClient[i] = new HashMap<REFITUniqueID, REFITRequest>();
            }
            this.recentlyRejected = new REFITRequest[REFITConfig.REJECT_THRESHOLD * 10];
            this.position = 0;
        }

        public void add(REFITRequest r) {
            if (recentlyRejected[position] != null) {
                REFITRequest old = recentlyRejected[position];
                rejectsPerClient[old.uid.nodeID].remove(old.uid);
            }
            recentlyRejected[position] = r;
            rejectsPerClient[r.uid.nodeID].put(r.uid, r);
            position++;
            if (position == recentlyRejected.length) position = 0;
        }

        public REFITRequest get(REFITUniqueID uid) {
            return rejectsPerClient[uid.nodeID].get(uid);
        }


}
