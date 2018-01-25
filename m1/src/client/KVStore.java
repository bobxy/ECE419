package client;

import java.io.IOException;
import java.net.Socket;
import java.util.HashSet;
import java.util.Set;
import Utilities.Utilities;
import org.apache.log4j.Logger;

import client.ClientSocketListenerInterface.SocketStatus;

import common.messages.KVMessage;
import common.messages.KVMessageC;
import common.messages.KVMessage.StatusType;

public class KVStore extends Thread implements KVCommInterface {
	/**
	 * Initialize KVStore with address and port of KVServer
	 * @param address the address of the KVServer
	 * @param port the port of the KVServer
	 */
	
	public String addr;
	public int portnum;
	private Socket clientSocket;
	private Logger logger = Logger.getRootLogger();
	private boolean running;
	private Set<ClientSocketListener> listeners;
	private ClientSocketListener listener;
	private MessageStream stream;
	private KVMessageC kvmsg;
	private boolean bReceived;
	private Utilities util;
	
	public KVStore(String address, int port) {
		// TODO Auto-generated method stub
		addr = address;
		portnum = port;
		listeners = new HashSet<ClientSocketListener>();
		bReceived = false;
		kvmsg = new KVMessageC();
		util = new Utilities();
	}

	@Override
	public void connect() throws Exception {							//add try-catch exception handler
		// TODO Auto-generated method stub
		clientSocket = new Socket(addr, portnum);
		listener = new ClientSocketListener(addr,portnum);
		stream = new MessageStream (clientSocket.getOutputStream(),clientSocket.getInputStream());
		addListener(listener);
		setRunning(true);
		logger.info("Connection established");
		System.out.println("Connection established");
	}

	@Override
	public void disconnect() {
		// TODO Auto-generated method stub
		logger.info("try to close connection ...");
		System.out.println("try to close connection ...");
		try {
			tearDownConnection();
			for(ClientSocketListener listener : listeners) {
				listener.handleStatus(SocketStatus.DISCONNECTED);
			}
		} catch (IOException ioe) {
			logger.error("Unable to close connection!");
		}
	}
	
	public void run() {
		try {
			while(isRunning()) {
				try {
					TextMessage latestMsg = stream.receiveMessage();
					for(ClientSocketListener listener : listeners) {
						kvmsg.StrToKVM(latestMsg.getMsg());
						bReceived = true;
					}
				} catch (IOException ioe) {
					if(isRunning()) {
						logger.error("Connection lost!");
						try {
							tearDownConnection();
							for(ClientSocketListener listener : listeners) {
								listener.handleStatus(
										SocketStatus.CONNECTION_LOST);
							}
						} catch (IOException e) {
							logger.error("Unable to close connection!");
						}
					}
				}				
			}
		} catch (Exception e){}
		finally {
			if(isRunning()) {
				disconnect();
			}
		}
	}
	
	public boolean isRunning() {
		return running;
	}
	
	public void setRunning(boolean run) {
		running = run;
	}
	
	
	private void tearDownConnection() throws IOException {
		setRunning(false);
		logger.info("tearing down the connection ...");
		if (clientSocket != null) {
			clientSocket.close();
			clientSocket = null;
		}
		if(stream != null)
		{
			stream.streamClose();
			stream = null;
		}
		logger.info("connection closed!");
	}
	
	private void addListener(ClientSocketListener listener){
		listeners.add(listener);
	}
	
	@Override
	public KVMessage put(String key, String value) throws Exception {
		//turn string into textmsg obj
		//send it to server
		try {
			if(util.InvaildKey(key) || value == null )
	    	{
	    		KVMessageC message = new KVMessageC(key, value, StatusType.PUT_ERROR);
	    		return message;
	    	}
			String msg = "3 " + key + " " + value; 
			stream.sendMessage(new TextMessage(msg));
		} catch (IOException e){
			System.out.println("Client> " + "Error! " +  "Unable to send message!");
			disconnect();
		}
		
		while(true)
		{
			Thread.sleep(1);
			if(bReceived)
			{
				bReceived = false;
				break;
			}
		}
		return kvmsg;
	}

	@Override
	public KVMessage get(String key) throws Exception {
		// TODO Auto-generated method stub 
		try {
			
			String msg = "0 " + key;
			stream.sendMessage(new TextMessage(msg));
		} catch (IOException e){
			System.out.println("Client> " + "Error! " +  "Unable to send message!");
			disconnect();
		}
		while(true)
		{
			Thread.sleep(1);
			if(bReceived)
			{
				bReceived = false;
				break;
			}
		}
		return kvmsg;
	}
}

