package app_kvServer;

import java.io.IOException;
import java.net.BindException;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;

import logger.LogSetup;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;

import common.ServerConfiguration;
import common.ServerConfigurations;

import client.KVStore;

import Utilities.Utilities;
import app_kvECS.ZKConnection;
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
	private KVStore ServerKVStore;
	private HashMap metadataMap;
	private boolean WriteLockFlag;
	private boolean HandleClientRequest;
	private ZKConnection zkC;
	private ZooKeeper zk;
	private ServerConfigurations SVCs;
	private Utilities myutilities;
	private String ServerMD5Hash;
	private boolean bReceivingData;
	private boolean bShouldBeRemoved;
	
	public KVServer(String name, String zkHostname, int zkPort) throws Exception {
		sHostname = name;
		sZKHostname = zkHostname;
		nZKPort = zkPort;
		
		HandleClientRequest=false;
		WriteLockFlag=false;
		myutilities = new Utilities();
		ServerMD5Hash="";
		bReceivingData = false;
		bShouldBeRemoved = false;
		update();
		
	}

	/*public ServerConfigurations constructMetaData() throws Exception
	{
		ServerConfigurations res=new ServerConfigurations();
		zkC= new ZKConnection();
		zk = zkC.connect(sZKHostname+":"+nZKPort);

		List<String> childrenList=zk.getChildren("/servers/", true);
		for(String child: childrenList)
		{
			String mypath="/servers/"+child;
			byte[] temp=zk.getData(mypath, true, zk.exists(mypath, true));
			ServerConfiguration svc= myutilities.ServerConfigByteArrayToSerializable(temp);
			if(svc.GetName()==sHostname)
			{
				ServerMD5Hash=svc.GetHashValue();
			}
			res.AddServer(svc);
		}
		return res;
	}*/
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
	

    
    private boolean isRunning() {
        return this.running;
    }
    
    
    
    public static void main(String[] args) throws Exception {
    	try {
    		System.out.println("SSH connection complete");
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
    public boolean initKVServer() throws NoSuchAlgorithmException, IOException{
    	//Initialize the KVServer with the metadata, it's local cache size, and the cache replacement strategy,
    	//and block it for client requests, i.e., all client requests are rejected with an SERVER_STOPPED error message;
    	//ECS requests have to be processed.
    	boolean res=false;
    	ServerConfiguration currentSVC=SVCs.FindServerByServerNameHash(ServerMD5Hash);
    	String strategy=currentSVC.GetStrategy();
    	int cacheSize=currentSVC.GetCacheSize();
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
    	
		nPort=currentSVC.GetPort();
        serverSocket = new ServerSocket(nPort);
        
    	return true;

    	
    }
    
    
	public void run() throws NoSuchAlgorithmException, IOException {
        
    	running = initKVServer();
        
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
	public void start() {
		// TODO
		//Starts the KVServer, all client requests and all ECS requests are processed.
		HandleClientRequest=true;

	}

    @Override
    public void stop() {
		// TODO
    	//Stops the KVServer, all client requests are rejected and only ECS requests are processed.
    	HandleClientRequest=false;
	}

    @Override
    public void shutDown() throws Exception {
    	//Exits the KVServer application.
    	//move data to next
    	//first target
    	ServerConfiguration tempSV=SVCs.FindNextHigher(ServerMD5Hash);
    	//send to tempSV
    	String[] hashRange=new String[2];
    	hashRange[0]=tempSV.GetLower();
    	hashRange[1]=tempSV.GetUpper();
    	boolean finished=moveData(hashRange,tempSV.GetHashValue());
    	
    	
    }
    @Override
    public void lockWrite() {
		// TODO
    	//Lock the KVServer for write operations.
    	//lock current server for write operations
    	WriteLockFlag=true;
	}

    @Override
    public void unlockWrite() {
		// TODO
    	//Unlock the KVServer for write operations.
    	WriteLockFlag=false;
	}

    @Override
    public boolean moveData(String[] hashRange, String targetName) throws Exception {
		// TODO
    	//Transfer a subset (range) of the KVServer's data to another KVServer 
    	//(reallocation before removing this server or adding a new KVServer to the ring); 
    	//send a notification to the ECS, if data transfer is completed.
		//create KVStore object here to take connections from client
    	//use targetName to find appropriate address, port
    	ServerConfiguration tempSVC=SVCs.FindServerByServerNameHash(targetName);
    	String TargetHostname=tempSVC.GetAddress();
    	int TargetPort=tempSVC.GetPort();
    	ServerKVStore = new KVStore(TargetHostname,TargetPort);
    	
    	// find all key value pairs fall into this range
    	String lowerbound=hashRange[0];
    	String upperbound=hashRange[1];
    	ArrayList<KeyValuePair> ListToMove=DO.get_subset(lowerbound, upperbound);
    	
    	for(KeyValuePair currentPair:ListToMove)
    	{
    		ServerKVStore.putNoCache(currentPair.getKey(), currentPair.getValue());
    	}
    	
    	ListToMove.clear();
    	
		return true;
	}
    
    @Override
    public void update() throws Exception {
    	//Update the metadata repository of this server
		ServerConfigurations res=new ServerConfigurations();
		zkC= new ZKConnection();
		zk = zkC.connect(sZKHostname+":"+nZKPort);

		List<String> childrenList=zk.getChildren("/servers/", true);
		for(String child: childrenList)
		{
			String mypath="/servers/"+child;
			byte[] temp=zk.getData(mypath, true, zk.exists(mypath, true));
			ServerConfiguration svc= myutilities.ServerConfigByteArrayToSerializable(temp);
			if(sHostname.equals(svc.GetName()))
			{
				ServerMD5Hash=svc.GetHashValue();
			}
			res.AddServer(svc);
		}
		SVCs=res;
    	
    }
    
    @Override
    public void putNoCache(String key,String value) throws IOException, NoSuchAlgorithmException
    {
    	DO.put(key,value);
    }
    
    public ServerConfigurations GetServerConfigurations()
    {
    	return SVCs;
    }
    
    public boolean IsResponsible(String sHash) throws Exception
    {
    	if(SVCs == null || SVCs.IsEmpty())
    		return false;
    	else
    		return ServerMD5Hash.equals(SVCs.FindServerForKey(sHash).GetHashValue());
    }
    
    public boolean IsLocked()
    {
    	return WriteLockFlag;
    }
    
    public boolean IsAcceptingRequest()
    {
    	return HandleClientRequest;
    }
    
    public void ReceivingData(boolean receivingData)
    {
    	bReceivingData = receivingData;
    	if(!receivingData && bShouldBeRemoved)
    	{
    		//Move data to next server. Do whatever
    	}
    }
}
