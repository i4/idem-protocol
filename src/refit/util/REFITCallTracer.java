package refit.util;

import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class REFITCallTracer {
	public static final REFITCallTracer instance = new REFITCallTracer();

	private ConcurrentHashMap<String, ConcurrentHashMap<String, Integer>> stacks = new ConcurrentHashMap<>();
	private ConcurrentHashMap<String, Long> callCount = new ConcurrentHashMap<>();
	private ConcurrentHashMap<String, Long> callDuration = new ConcurrentHashMap<>();

	public void trackStack(String name) {
		StackTraceElement[] stes = Thread.currentThread().getStackTrace();
		StringBuilder s = new StringBuilder();
		for (int i = 2; i < stes.length; i++) {
			s.append(stes[i].toString());
			s.append("\n");
		}
		ConcurrentHashMap<String, Integer> map = stacks.get(name);
		if (map == null) {
			// make sure that a HashMap will exist, then get the actual value
			stacks.putIfAbsent(name, new ConcurrentHashMap<>());
			map = stacks.get(name);
		}
		map.put(s.toString(), map.getOrDefault(s.toString(), 0) + 1);
	}


	public void dumpStacks() {
		for (Map.Entry<String, ConcurrentHashMap<String, Integer>> entry : stacks.entrySet()) {
			dumpStackMap(entry.getKey(), entry.getValue());
		}

		for (Map.Entry<String, Long> entry : callCount.entrySet()) {
			Long count = entry.getValue();
			Long duration = callDuration.getOrDefault(entry.getKey(), (long) 0);
			REFITLogger.logPlain(entry.getKey() + " took " + duration + "ns for " + count + " iterations. Avg: " + ((double) duration / count / 1e6) + "ms");
		}
		callCount.clear();
		callDuration.clear();
	}

	private void dumpStackMap(String key, Map<String, Integer> map) {
		REFITLogger.logPlain("Key: " + key);
		ArrayList<Map.Entry<String, Integer>> list = new ArrayList<>(map.entrySet());
		list.sort(Map.Entry.comparingByValue());

		for (Map.Entry<String, Integer> entry : list) {
			REFITLogger.logPlain(Integer.toString(entry.getValue()));
			REFITLogger.logPlain(entry.getKey());
			REFITLogger.logPlain("");
		}
	}

	public void trackCallDuration(String name, long nanoTime) {
		callCount.put(name, callCount.getOrDefault(name, (long) 0) + 1);
		callDuration.put(name, callDuration.getOrDefault(name, (long) 0) + nanoTime);
	}
}
