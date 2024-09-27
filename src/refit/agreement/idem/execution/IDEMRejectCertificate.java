package refit.agreement.idem.execution;

import refit.agreement.idem.IDEMMessage.IDEMReject;
import refit.config.REFITConfig;
import refit.message.REFITUniqueID;

public class IDEMRejectCertificate {
    
    public enum RejectStatus {
        UNKNOWN,
        DEFERRED,
        FULLY_REJECTED
    }

    private REFITUniqueID uid;
    private boolean[] rejects;
    private RejectStatus rejected;
    private int threshold;

    public IDEMRejectCertificate(int threshold) {
        this.rejects = new boolean[REFITConfig.TOTAL_NR_OF_REPLICAS];
        this.rejected = RejectStatus.UNKNOWN;
        this.threshold = threshold;
    }

    public void init(REFITUniqueID uid) {
        // Clear existing rejects
        for (int i=0; i<rejects.length; i++) {
            this.rejects[i] = false;
        }
        this.rejected = RejectStatus.UNKNOWN;
        this.uid = uid;
    }

    public void reject(IDEMReject reject) {
        if (!this.uid.equals(reject.uid) || rejects[reject.from] == true) return;
        rejects[reject.from] = true;
        int count = 0;
        for (boolean r : rejects) {
            if (r) count++;
        }
        if (count == REFITConfig.TOTAL_NR_OF_REPLICAS) {
            rejected = RejectStatus.FULLY_REJECTED;
        } else if (count >= threshold) {
            rejected = RejectStatus.DEFERRED;
        }
    }

    public boolean fullyRejected() {
        return rejected == RejectStatus.FULLY_REJECTED;
    }

    public boolean deferred() {
        return rejected == RejectStatus.DEFERRED;
    }
}
