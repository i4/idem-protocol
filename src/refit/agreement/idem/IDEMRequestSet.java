package refit.agreement.idem;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeSet;

import refit.config.REFITConfig;
import refit.message.REFITUniqueID;

public class IDEMRequestSet<T> extends HashMap<REFITUniqueID, T> {
    
    	private final TreeSet<REFITUniqueID>[] requestsPerClient;

        public IDEMRequestSet() {
            this.requestsPerClient = new TreeSet[REFITConfig.TOTAL_NR_OF_CLIENTS];
            for (int i = 0; i < requestsPerClient.length; i++) {
                this.requestsPerClient[i] = new TreeSet<REFITUniqueID>((a,b) -> (Long.compare(a.seqNr, b.seqNr)));
            }
        }

        @Override
        public T put(REFITUniqueID uid, T r) {
            requestsPerClient[uid.nodeID].add(uid);
            return super.put(uid, r);
        }

        @Override
        public void putAll(Map<? extends REFITUniqueID, ? extends T> requests) {
            for (REFITUniqueID uid : requests.keySet()) {
                requestsPerClient[uid.nodeID].add(uid);
            }
            super.putAll(requests);
        }

        @Override
        public void clear() {
            for (TreeSet<REFITUniqueID> s : requestsPerClient) {
                s.clear();
            }
            super.clear();
        }

        @Override
        public T remove(Object o) {
            REFITUniqueID uid = (REFITUniqueID) o;
            // Removes not only this particular request but all requests
            // of the client that are older.
            T r = null;
            ArrayList<REFITUniqueID> toRemove = new ArrayList<>();
            for (REFITUniqueID id : requestsPerClient[uid.nodeID]) {
                if (id.seqNr < uid.seqNr) {
                    super.remove(id);
                    toRemove.add(id);
                } else if (id.seqNr == uid.seqNr) {
                    r = super.remove(id);
                    toRemove.add(id);
                }  else {
                    break;
                }
            }
            for (REFITUniqueID id : toRemove) {
                requestsPerClient[uid.nodeID].remove(id);
            }
            return r;
        }

        public void removeExecuted(long[] executed) {
            for (int i=0; i < executed.length; i++) {
                ArrayList<REFITUniqueID> toRemove = new ArrayList<>();
                for (REFITUniqueID id : requestsPerClient[i]) {
                    if (id.seqNr < executed[i]) {
                        super.remove(id);
                        toRemove.add(id);
                    } else {
                        break;
                    }
                }
                for (REFITUniqueID id : toRemove) {
                    requestsPerClient[i].remove(id);
                }
                toRemove.clear();
            }
        }
}
