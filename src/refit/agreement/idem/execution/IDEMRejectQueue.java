package refit.agreement.idem.execution;

import java.util.HashMap;

import refit.agreement.idem.IDEMMessage.IDEMReject;
import refit.config.REFITConfig;
import refit.message.REFITRequest;
import refit.message.REFITUniqueID;
import refit.stage.REFITReplicaContext;
import refit.util.REFITLogger;

public class IDEMRejectQueue {

    private static class QueueElement {
        public REFITRequest request;
        public long finalRejectTime = Long.MAX_VALUE;
        public boolean skip;

        public void set(REFITRequest request) {
            this.request = request;
            this.finalRejectTime = System.nanoTime() + REFITConfig.REJECT_GRACE;
            this.skip = false;
        }

        public void clear() {
            this.request = null;
            this.finalRejectTime = Long.MAX_VALUE;
            this.skip = false;
        }
    }

    private final REFITReplicaContext replica;
    public IDEMRecentlyRejected recentlyRejected;

    private QueueElement[] queue;
    public int head;
    public int tail;

    private final HashMap<REFITUniqueID, Integer> indexMap;

    public IDEMRejectQueue(REFITReplicaContext replica) {
        this.queue = new QueueElement[REFITConfig.TOTAL_NR_OF_CLIENTS];
        for (int i=0; i < queue.length; i++) {
            queue[i] = new QueueElement();
        }
        this.head = 0;
        this.tail = 0;
        this.replica = replica;
        this.recentlyRejected = new IDEMRecentlyRejected();
        this.indexMap = new HashMap<>();
    }

    public void add(REFITRequest request) {
        cleanup();
        if (queue[tail].request != null) {
            // Forward head as we overwrite current head
            head = (head+1 == queue.length ? 0 : head+1);
            reject(queue[tail].request);
            indexMap.remove(queue[tail].request.uid);
        }
        queue[tail].set(request);
        indexMap.put(request.uid, tail);
        tail = (tail+1 == queue.length ? 0 : tail+1);
    }

    public REFITRequest get() {
        cleanup();
        // Potentially skip removed entries
        while (queue[head].skip) {
            head = (head+1 == queue.length ? 0 : head+1);
        }
        REFITRequest request = queue[head].request;
        if (request != null) {
            indexMap.remove(request.uid);
            queue[head].clear();
            head = (head+1 == queue.length ? 0 : head+1);
        }
        return request;
    }

    public void remove(REFITUniqueID uid) {
        Integer index = indexMap.get(uid);
        if (index == null) return;
        indexMap.remove(uid);
        queue[index].skip = true;
    }

    public void cleanup() {
        long now = System.nanoTime();
        int i = head;
        while (i != tail) {
            if(queue[i].finalRejectTime < now) {
                reject(queue[i].request);
                indexMap.remove(queue[i].request.uid);
                queue[i].clear();
                head = (head+1 == queue.length ? 0 : head+1);
            } else if (!queue[i].skip) {
                break;
            }
            i = (i+1 == queue.length ? 0 : i+1);
        }
    }

    private void reject(REFITRequest r) {
        this.recentlyRejected.add(r);
        if (REFITLogger.LOG_EXECUTION) REFITLogger.logExecution(this, "Finally rejecting request " + r.uid);

        IDEMReject reject = new IDEMReject(r.uid, replica.id);
        reject.serializeMessage();
        reject.markVerified();
        replica.sendMessageToClient(reject, r.uid.nodeID);
    }

}