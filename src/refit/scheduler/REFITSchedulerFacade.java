package refit.scheduler;

import java.io.IOException;
import java.nio.channels.SelectableChannel;
import java.nio.channels.SelectionKey;

public interface REFITSchedulerFacade {
	SelectionKey registerIOTask(REFITSchedulerTask task, SelectableChannel channel, int operations) throws IOException;

	void deregisterIOTask(SelectableChannel channel);

	void notifyProgress(REFITSchedulerTask task);

	REFITTimer newTimer(REFITTimer.TimerCallback callback);

	void queueTimer(REFITTimer timer);

	void stopTimer(REFITTimer timer);
}
