package app_kvServer;

import java.io.IOException;
import java.net.BindException;
import java.net.ServerSocket;
import java.net.Socket;

import logger.LogSetup;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import app_kvServer.IKVServer.CacheStrategy;

public class KVServer implements IKVServer {

	/**
	 * Start KV Server with selected name
	 * @param name			unique name of server
	 * @param zkHostname	hostname where zookeeper is running
	 * @param zkPort		port where zookeeper is running
	 */
	private static Logger logger = Logger.getRootLogger();
	private String sHostname;
	private int nPort;
	private int nCacheSize;
	IKVServer.CacheStrategy CacheStrategy;
	private ServerSocket serverSocket;
    private boolean running;
	private Cache cache;
	private diskOperation DO;
	private String sZKHostname;
	private int nZKPort;
	
	public KVServer(String name, String zkHostname, int zkPort) {
		sHostname = name;
		sZKHostname = zkHostname;
		nZKPort = zkPort;
	}

	@Override
	public int getPort(){
		return nPort;
	}

	@Override
    public String getHostname(){
		return sHostname;
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
		return (CacheStrategy != IKVServer.CacheStrategy.None) && cache.inCache(key);
	}

	@Override
    public String getKV(String key) throws Exception{
		if(inCache(key))
		{
			return cache.get(key);
		}
		else if(inStorage(key))
		{
			String sVal = DO.get(key);
			cache.put(key, sVal);
			return sVal;
		}
		return "";
	}

	@Override
    public void putKV(String key, String value) throws Exception{
		DO.put(key, value);
		cache.put(key, value);
	}

	@Override
    public void clearCache(){
		cache.clearCache();
	}

	@Override
    public void clearStorage(){
		DO.clearStorage();
	}

	@Override
    public void kill(){
		// TODO Auto-generated method stub
		running = false;
        try {
			serverSocket.close();
		} catch (IOException e) {
			logger.error("Error! " +
					"Unable to close socket on port: " + nPort, e);
		}
	}

	@Override
    public void close(){
		// TODO Auto-generated method stub
		running = false;
        try {
			serverSocket.close();
		} catch (IOException e) {
			logger.error("Error! " +
					"Unable to close socket on port: " + nPort, e);
		}
	}
	
	public void run() {
        
    	running = initializeServer();
        
        if(serverSocket != null) {
	        while(isRunning()){
	            try {
	                Socket client = serverSocket.accept();                
	                ClientConnection connection = 
	                		new ClientConnection(client, this);
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
            nPort = serverSocket.getLocalPort();
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
    
    public void Configure()
    {
    	//create a listener on port nZKPort, wait for zoo keeper to send all the configurations
    	//set all the values and configure the server according to all the values
    	/*
    	nPort = port;
		nCacheSize = cacheSize;
		DO = new diskOperation();
		DO.load_lookup_table();
		if(strategy.equals("FIFO"))
			CacheStrategy = IKVServer.CacheStrategy.FIFO;
		else if(strategy.equals("LRU"))
			CacheStrategy = IKVServer.CacheStrategy.LRU;
		else if(strategy.equals("LFU"))
			CacheStrategy = IKVServer.CacheStrategy.LFU;
		else
			CacheStrategy = IKVServer.CacheStrategy.None;
		cache = new Cache(CacheStrategy, cacheSize);
		*/
    }
    
    public static void main(String[] args) {
    	try {
			new LogSetup("logs/server.log", Level.ALL);
			if(args.length != 4) {
				System.out.println("Error! Invalid number of arguments!");
				System.out.println("Usage: Server <port> <cache_size> <cache_strategy>!");
			} else {
				String sName = args[0];
				String sZKName = args[1];
				int zkPort = Integer.parseInt(args[2]);
				KVServer Server = new KVServer(sName, sZKName, zkPort);
				Server.Configure();
				Server.start();
			}
		} catch (IOException e) {
			System.out.println("Error! Unable to initialize logger!");
			e.printStackTrace();
			System.exit(1);
		} catch (NumberFormatException nfe) {
			System.out.println("Error! Invalid argument <port>! Not a number!");
			System.out.println("Usage: Server <port>!");
			System.exit(1);
		}
    }

	@Override
	public void start() {
		// TODO
	}

    @Override
    public void stop() {
		// TODO
	}

    @Override
    public void lockWrite() {
		// TODO
	}

    @Override
    public void unlockWrite() {
		// TODO
	}

    @Override
    public boolean moveData(String[] hashRange, String targetName) throws Exception {
		// TODO
		return false;
	}
}
