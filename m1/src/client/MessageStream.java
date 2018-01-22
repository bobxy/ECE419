package client;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class MessageStream {
	
	private OutputStream output;
 	private InputStream input;
	
	private static final int BUFFER_SIZE = 1024;
	private static final int DROP_SIZE = 1024 * BUFFER_SIZE;
	

	public MessageStream(OutputStream out, InputStream inp) {
		
		output = out;
		input = inp;
		
	}
	
	public void streamClose() throws IOException{
		output.close();
		input.close();
	}
	
	public void sendMessage(TextMessage msg) throws IOException {
		byte[] msgBytes = msg.getMsgBytes();
		output.write(msgBytes, 0, msgBytes.length);
		output.flush();
		//logger.info("Send message:\t '" + msg.getMsg() + "'");
    }
	
	
	public TextMessage receiveMessage() throws IOException {
		
		int index = 0;
		byte[] msgBytes = null, tmp = null;
		byte[] bufferBytes = new byte[BUFFER_SIZE];
		
		/* read first char from stream */
		byte read = (byte) input.read();	
		boolean reading = true;
		
		while(read != 13 && reading) {/* carriage return */
			/* if buffer filled, copy to msg array */
			if(index == BUFFER_SIZE) {
				if(msgBytes == null){
					tmp = new byte[BUFFER_SIZE];
					System.arraycopy(bufferBytes, 0, tmp, 0, BUFFER_SIZE);
				} else {
					tmp = new byte[msgBytes.length + BUFFER_SIZE];
					System.arraycopy(msgBytes, 0, tmp, 0, msgBytes.length);
					System.arraycopy(bufferBytes, 0, tmp, msgBytes.length,
							BUFFER_SIZE);
				}

				msgBytes = tmp;
				bufferBytes = new byte[BUFFER_SIZE];
				index = 0;
			} 
			
			/* only read valid characters, i.e. letters and numbers */
			if((read > 31 && read < 127)) {
				bufferBytes[index] = read;
				index++;
			}
			
			/* stop reading is DROP_SIZE is reached */
			if(msgBytes != null && msgBytes.length + index >= DROP_SIZE) {
				reading = false;
			}
			
			/* read next char from stream */
			read = (byte) input.read();
		}
		
		if(msgBytes == null){
			tmp = new byte[index];
			System.arraycopy(bufferBytes, 0, tmp, 0, index);
		} else {
			tmp = new byte[msgBytes.length + index];
			System.arraycopy(msgBytes, 0, tmp, 0, msgBytes.length);
			System.arraycopy(bufferBytes, 0, tmp, msgBytes.length, index);
		}
		
		msgBytes = tmp;
		
		/* build final String */
		TextMessage msg = new TextMessage(msgBytes);
		//logger.info("Receive message:\t '" + msg.getMsg() + "'");
		return msg;
    }
 	
	
	

}
