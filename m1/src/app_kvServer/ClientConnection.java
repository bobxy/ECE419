package app_kvServer;

import java.io.InputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import client.TextMessage;
import common.messages.KVMessage;
import common.messages.KVMessage.StatusType;
import common.messages.KVMessageC;
import org.apache.log4j.*;


/**
 * Represents a connection end point for a particular client that is 
 * connected to the server. This class is responsible for message reception 
 * and sending. 
 * The class also implements the echo functionality. Thus whenever a message 
 * is received it is going to be echoed back to the client.
 */
public class ClientConnection implements Runnable {

	private static Logger logger = Logger.getRootLogger();
	
	private boolean isOpen;
	private static final int BUFFER_SIZE = 1024;
	private static final int DROP_SIZE = 128 * BUFFER_SIZE;
	
	private Socket clientSocket;
	private InputStream input;
	private OutputStream output;
	private KVServer kvs;
	/**
	 * Constructs a new CientConnection object for a given TCP socket.
	 * @param clientSocket the Socket object for the client connection.
	 */
	public ClientConnection(Socket clientSocket, KVServer server) {
		this.clientSocket = clientSocket;
		this.kvs = server;
		this.isOpen = true;
	}
	
	/**
	 * Initializes and starts the client connection. 
	 * Loops until the connection is closed or aborted by the client.
	 */
	public void run(){
		try {
			output = clientSocket.getOutputStream();
			input = clientSocket.getInputStream();
		
			while(isOpen) {
				KVMessageC message = new KVMessageC();
				try {
					TextMessage latestMsg = receiveMessage();
					
					if (latestMsg.getMsg().equals("")){
						sendMessage(new TextMessage(latestMsg.getMsg()));
					}
					else
					{
					message.StrToKVM(latestMsg.getMsg());
					String sKey = message.getKey();
					String sValue = "";
					String sRet = "";
					if(message.getStatus() == StatusType.PUT)
					{
						boolean bOld = kvs.inStorage(sKey);
						boolean bDelete = (message.getValue().length() == 0);
						if(!bOld && bDelete)
							sRet = "8 " + sKey;
						else
						{
							kvs.putKV(sKey, message.getValue());
						
							int nStatus = -1;
							if(bOld && bDelete)
								nStatus = 7;
							else if(bOld && !bDelete)
								nStatus = 5;
							else
								nStatus = 4;
							sRet = Integer.toString(nStatus) + " " + sKey + " " + message.getValue();
						}
							
					}
					else if(message.getStatus() == StatusType.GET)
					{
						sValue = kvs.getKV(sKey);
						if(sValue.length() == 0)
						{
							sRet = "1 " + sKey;
						}
						else
							sRet = "2 " + sKey + " " + sValue;
					}
					
					
					sendMessage(new TextMessage(sRet));
					
				/* connection either terminated by the client or lost due to 
				 * network problems*/	
				} 
				}catch (IOException ioe) {
					logger.error("Error! Connection lost!");
					isOpen = false;
				} 
				catch (Exception e)
				{
					logger.error("Error!", e);
				}				
			}
			
		} catch (IOException ioe) {
			logger.error("Error! Connection could not be established!", ioe);
			
		}
		finally {
			
			try {
				if (clientSocket != null) {
					input.close();
					output.close();
					clientSocket.close();
				}
			} catch (IOException ioe) {
				logger.error("Error! Unable to tear down connection!", ioe);
			}
		}
	}
	
	/**
	 * Method sends a TextMessage using this socket.
	 * @param msg the message that is to be sent.
	 * @throws IOException some I/O error regarding the output stream 
	 */
	public void sendMessage(TextMessage msg) throws IOException {
		byte[] msgBytes = msg.getMsgBytes();
		output.write(msgBytes, 0, msgBytes.length);
		output.flush();
		logger.info("SEND \t<" 
				+ clientSocket.getInetAddress().getHostAddress() + ":" 
				+ clientSocket.getPort() + ">: '" 
				+ msg.getMsg() +"'");
    }
	
	
	private TextMessage receiveMessage() throws IOException {
		
		int index = 0;
		byte[] msgBytes = null, tmp = null;
		byte[] bufferBytes = new byte[BUFFER_SIZE];
		
		/* read first char from stream */
		byte read = (byte) input.read();	
		boolean reading = true;
		
		logger.info("First Char: " + read);
		//Check if stream is closed (read returns -1)
		if (read == -1){
			TextMessage msg = new TextMessage("");
			return msg;
		}

		while(/*read != 13  && */ read != 10 && read !=-1 && reading) {/* CR, LF, error */
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
			
			/* only read valid characters, i.e. letters and constants */
			bufferBytes[index] = read;
			index++;
			
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
		logger.info("RECEIVE \t<" 
				+ clientSocket.getInetAddress().getHostAddress() + ":" 
				+ clientSocket.getPort() + ">: '" 
				+ msg.getMsg().trim() + "'");
		return msg;
    }
	
	
	
}