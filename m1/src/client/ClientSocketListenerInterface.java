package client;

public interface ClientSocketListenerInterface {

	public enum SocketStatus{CONNECTED, DISCONNECTED, CONNECTION_LOST};
	
	public String handleNewMessage(TextMessage msg);
	
	public void handleStatus(SocketStatus status);
}
