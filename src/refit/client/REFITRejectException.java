package refit.client;

public class REFITRejectException extends Exception {

    public boolean full;

    public REFITRejectException(boolean full) {
        super("Request rejected!");
        this.full = full;
    }
}
