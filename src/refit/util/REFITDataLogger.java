package refit.util;

import java.io.*;
import java.util.concurrent.ConcurrentLinkedQueue;

public class REFITDataLogger implements Runnable {
	private final File logFile;
	private final ConcurrentLinkedQueue<String> logQueue = new ConcurrentLinkedQueue<>();


	public REFITDataLogger(File logFile) {
		this.logFile = logFile;
	}

	@Override
	public void run() {
		OutputStream out;
		try {
			out = new BufferedOutputStream(new FileOutputStream(logFile));
		} catch (FileNotFoundException e) {
			e.printStackTrace();
			REFITLogger.logError(this, "Failed to initialize data logger");
			System.exit(1);
			return;
		}

		int emptyCounter = 0;
		while (!Thread.currentThread().isInterrupted()) {
			String nextMessage = logQueue.poll();
			if (nextMessage == null) {
				emptyCounter++;
				try {
					Thread.sleep(emptyCounter);
				} catch (InterruptedException ignored) {
					// keep interrupt flag
					Thread.currentThread().interrupt();
				}
				continue;
			}
			emptyCounter = 0;
			if (nextMessage.isEmpty()) {
				try {
					out.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
				return;
			}

			byte[] data = nextMessage.getBytes();
			try {
				out.write(data);
				out.write('\n');
			} catch (IOException e) {
				e.printStackTrace();
				REFITLogger.logError(this, "Data logger failed");
				System.exit(1);
				return;
			}
		}
	}

	public void log(String data) {
		logQueue.add(data);
	}

	public void shutdown() {
		logQueue.add("");
	}

	public static REFITDataLogger spawn(String filename) {
		REFITDataLogger logger = new REFITDataLogger(new File(filename));
		Thread thread = new Thread(logger);
		thread.start();
		Runtime.getRuntime().addShutdownHook(new Thread(() -> {
			logger.shutdown();
			try {
				thread.join();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}));
		return logger;
	}

	public static void main(String[] args) throws IOException, InterruptedException {
		File tmpFile = File.createTempFile("refit", ".trace");

		// setup logger
		REFITDataLogger logger = new REFITDataLogger(tmpFile);
		Thread logThread = new Thread(logger);
		logThread.setDaemon(true);
		logThread.start();

		long timeBefore = REFITTime.nanoTime.getAsLong();
		for (int i = 0; i < 3000000; i++) {
			logger.log("Counter tests with a very long output string test test test " + timeBefore + ' ' + i);
			if ((i % 10000) == 0) {
				System.out.println("" + i);
			}
		}
		logThread.interrupt();
		logThread.join(1000);
		long timeAfter = REFITTime.nanoTime.getAsLong();
		System.out.printf("Time %f ms\n", (timeAfter - timeBefore) * 1e-6);

		// cleanup
		if (!tmpFile.delete()) {
			System.out.println("Failed to cleanup logfile");
		}
	}
}
