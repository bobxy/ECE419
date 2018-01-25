package client;

public class ClientSocketListener implements ClientSocketListenerInterface {

	private String serverAddress;
	private int serverPort;
	
	public ClientSocketListener(String addr, int port) {
		this.serverAddress = addr;
		this.serverPort = port;
	}
	
	@Override
	public String handleNewMessage(TextMessage msg) {
		//if(!stop) {
			//System.out.println(msg.getMsg());
			//System.out.print(PROMPT);
		//}
		return msg.getMsg();
		
	}
	
	@Override
	public void handleStatus(SocketStatus status) {
		if(status == SocketStatus.CONNECTED) {

		} else if (status == SocketStatus.DISCONNECTED) {
			System.out.println("Connection terminated: " 
					+ serverAddress + " / " + serverPort);
			
		} else if (status == SocketStatus.CONNECTION_LOST) {
			System.out.println("Connection lost: " 
					+ serverAddress + " / " + serverPort);
		}
		
	}


}
