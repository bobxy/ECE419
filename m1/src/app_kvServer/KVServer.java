package app_kvServer;

import java.net.BindException;
import java.net.ServerSocket;
import java.net.Socket;
import java.io.IOException;

import logger.LogSetup;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public class KVServer implements IKVServer {

	/**
	 * Start KV Server at given port
	 * @param port given port for storage server to operate
	 * @param cacheSize specifies how many key-value pairs the server is allowed
	 *           to keep in-memory
	 * @param strategy specifies the cache replacement strategy in case the cache
	 *           is full and there is a GET- or PUT-request on a key that is
	 *           currently not contained in the cache. Options are "FIFO", "LRU",
	 *           and "LFU".
	 */
	private static Logger logger = Logger.getRootLogger();
	private int nPort;
	private int nCacheSize;
	IKVServer.CacheStrategy CacheStrategy;
	private ServerSocket serverSocket;
    private boolean running;
	private Cache cache;
	private diskOperation DO;
	
	public KVServer(int port, int cacheSize, String strategy) {
		nPort = port;
		nCacheSize = cacheSize;
		DO = new diskOperation();
		if(strategy.equals("FIFO"))
			CacheStrategy = IKVServer.CacheStrategy.FIFO;
		else if(strategy.equals("LRU"))
			CacheStrategy = IKVServer.CacheStrategy.LRU;
		else if(strategy.equals("LFU"))
			CacheStrategy = IKVServer.CacheStrategy.LFU;
		else
			CacheStrategy = IKVServer.CacheStrategy.None;
		cache = new Cache(CacheStrategy, cacheSize);
	}

	@Override
	public int getPort(){
		return nPort;
	}

	@Override
    public String getHostname(){
		// TODO Auto-generated method stub
		return null;
	}

	@Override
    public CacheStrategy getCacheStrategy(){
		return CacheStrategy;
	}

	@Override
    public int getCacheSize(){
		return nCacheSize;
	}

	@Override
    public boolean inStorage(String key){
		return DO.inStorage(key);
	}

	@Override
    public boolean inCache(String key){
		return cache.inCache(key);
	}

	@Override
    public String getKV(String key) throws Exception{
		if(inCache(key))
			return cache.get(key);
		else if(inStorage(key))
		{
			//String sVal = DO.get(key);
			//cache.put(key, sVal);
			//return DO.get(key);
		}
		return "";
	}

	@Override
    public void putKV(String key, String value) throws Exception{
		// TODO Auto-generated method stub
	}

	@Override
    public void clearCache(){
		cache.clearCache();
	}

	@Override
    public void clearStorage(){
		//DO.clearStorage();
	}

	@Override
    public void kill(){
		// TODO Auto-generated method stub
	}

	@Override
    public void close(){
		// TODO Auto-generated method stub
	}
	
	public void run() {
        
    	running = initializeServer();
        
        if(serverSocket != null) {
	        while(isRunning()){
	            try {
	                Socket client = serverSocket.accept();                
	                ClientConnection connection = 
	                		new ClientConnection(client);
	                new Thread(connection).start();
	                
	                logger.info("Connected to " 
	                		+ client.getInetAddress().getHostName() 
	                		+  " on port " + client.getPort());
	            } catch (IOException e) {
	            	logger.error("Error! " +
	            			"Unable to establish connection. \n", e);
	            }
	        }
        }
        logger.info("Server stopped.");
    }
    
    private boolean isRunning() {
        return this.running;
    }
    
    private boolean initializeServer() {
    	logger.info("Initialize server ...");
    	try {
            serverSocket = new ServerSocket(nPort);
            logger.info("Server listening on port: " 
            		+ serverSocket.getLocalPort());    
            return true;
        
        } catch (IOException e) {
        	logger.error("Error! Cannot open server socket:");
            if(e instanceof BindException){
            	logger.error("Port " + nPort + " is already bound!");
            }
            return false;
        }
    }
}
