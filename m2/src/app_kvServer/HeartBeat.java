package app_kvServer;

public class HeartBeat extends Thread{
	KVServer server;
	
	public HeartBeat(KVServer kv){
		server = kv;
	}
	
	public void run()
	{
		while(server != null)
		{
			try {
				server.HeartBeat();
				Thread.sleep(10000);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
}
