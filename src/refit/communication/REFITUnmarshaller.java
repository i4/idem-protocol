package refit.communication;

import java.nio.ByteBuffer;
import java.util.function.Consumer;


public class REFITUnmarshaller {

	public static final REFITUnmarshaller instance = new REFITUnmarshaller();


	public boolean unmarshalMessage(ByteBuffer buffer, Consumer<ByteBuffer> messageHandler) {
		// Analyze size field in header
		if (buffer.remaining() < Integer.BYTES) return false;
		int messageSize = buffer.getInt(buffer.position());
		if (buffer.remaining() < messageSize) return false;

		// Pass to message handler
		int oldLimit = buffer.limit();
		int processedMark = buffer.position() + messageSize;
		buffer.limit(processedMark);
		messageHandler.accept(buffer);

		// Move to next message
		buffer.position(processedMark);
		buffer.limit(oldLimit);

		return true;
	}

}
