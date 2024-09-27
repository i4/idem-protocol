package refit.util;

import java.io.PrintWriter;
import java.io.StringWriter;

public class REFITStackTrace {
	public static String toString(Exception e) {
		StringWriter sw = new StringWriter();
		e.printStackTrace(new PrintWriter(sw));
		return sw.toString();
	}
}
