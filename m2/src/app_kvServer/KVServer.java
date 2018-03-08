package app_kvServer;

import java.io.IOException;
import java.net.BindException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;
import java.util.concurrent.locks.ReentrantLock;

import logger.LogSetup;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import client.KVStore;

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
	private KVStore KVS;
	private HashMap metadataMap;
	private ReentrantLock ServerLock;
	public KVServer(String name, String zkHostname, int zkPort) {
		sHostname = name;
		sZKHostname = zkHostname;
		nZKPort = zkPort;
		ServerLock=new ReentrantLock();
    	//tcp connection to zookeeper
    	//get metadata from zk
		//parse metadata
		//cache size cache strategy
		//set watcher node
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
        
    }
    
    private boolean isRunning() {
        return this.running;
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
				//TODO
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
    public boolean initKVServer(HashMap metadata,int cacheSize, String replacementStrategy){
    	//Initialize the KVServer with the metadata, it's local cache size, and the cache replacement strategy,
    	//and block it for client requests, i.e., all client requests are rejected with an SERVER_STOPPED error message;
    	//ECS requests have to be processed.
		DO = new diskOperation();
		DO.load_lookup_table();
		if(replacementStrategy.equals("FIFO"))
			CacheStrategy = IKVServer.CacheStrategy.FIFO;
		else if(replacementStrategy.equals("LRU"))
			CacheStrategy = IKVServer.CacheStrategy.LRU;
		else if(replacementStrategy.equals("LFU"))
			CacheStrategy = IKVServer.CacheStrategy.LFU;
		else
			CacheStrategy = IKVServer.CacheStrategy.None;
		cache = new Cache(CacheStrategy, cacheSize);

    	
    }
	@Override
	public void start() {
		// TODO
		//Starts the KVServer, all client requests and all ECS requests are processed.

    	running = initKVServer(metadataMap,nCacheSize,CacheStrategy);
        
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

    @Override
    public void stop() {
		// TODO
    	//Stops the KVServer, all client requests are rejected and only ECS requests are processed.
		running = false;
        try {
			serverSocket.close();
		} catch (IOException e) {
			logger.error("Error! " +
					"Unable to close socket on port: " + nPort, e);
		}
	}

    @Override
    public void shutDown() {
    	//Exits the KVServer application.
    	//move data to next
    }
    @Override
    public void lockWrite() {
		// TODO
    	//Lock the KVServer for write operations.
    	//lock current server for write operations
    	ServerLock.lock();
	}

    @Override
    public void unlockWrite() {
		// TODO
    	//Unlock the KVServer for write operations.
    	ServerLock.unlock();
	}

    @Override
    public boolean moveData(String[] hashRange, String targetName) throws Exception {
		// TODO
    	//Transfer a subset (range) of the KVServer's data to another KVServer 
    	//(reallocation before removing this server or adding a new KVServer to the ring); 
    	//send a notification to the ECS, if data transfer is completed.
		//create KVStore object here to take connections from client
    	//use targetName to find appropriate address, port
    	String TargetHostname;
    	int TargetPort;
		KVS = new KVStore(TargetHostname, TargetPort);
		KVS.connect();
		//determine which key value pairs need to move.
		//use kv message to send all the pairs to target
		ValidateReturnedMessage(put(key, value));
		return false;
	}
    
    @Override
    public void update(String metadata) {
    	//Update the metadata repository of this server
    	//call parser from here
    	
    }
    
    @Override
    public void putKVonly(String key,String value) throws IOException
    {
    	DO.put(key,value);
    }
}
