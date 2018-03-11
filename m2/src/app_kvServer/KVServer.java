package app_kvServer;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
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
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.Watcher.Event;

import common.ServerConfiguration;
import common.ServerConfigurations;

import client.KVStore;

import Utilities.Utilities;
import Utilities.Utilities.servStrategy;
import app_kvECS.ZKConnection;
import app_kvServer.IKVServer.CacheStrategy;

public class KVServer implements IKVServer, Runnable {

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
	//private String ServerMD5Hash;
	private boolean bReceivingData;
	private boolean bShouldBeRemoved;
	private String ServerPath;
	private ServerConfiguration currentSVC;
	public KVServer(String name, String zkHostname, int zkPort) throws Exception {
		sHostname = name;
		sZKHostname = zkHostname;
		nZKPort = zkPort;
		
		HandleClientRequest=false;
		WriteLockFlag=false;
		myutilities = new Utilities();
		bReceivingData = false;
		bShouldBeRemoved = false;
		
		ServerPath="";
		zkC= new ZKConnection();
		zk = zkC.connect(sZKHostname+":"+nZKPort);
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
			if(args.length != 3) {
				System.out.println("Error! Invalid number of arguments!");
				System.out.println("Usage: Server <name> <zkaddress> <zkport>!");
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
    public boolean initKVServer() throws Exception{
    	//Initialize the KVServer with the metadata, it's local cache size, and the cache replacement strategy,
    	//and block it for client requests, i.e., all client requests are rejected with an SERVER_STOPPED error message;
    	//ECS requests have to be processed.
    	update();
    	currentSVC=SVCs.FindServerByServerNameHash(myutilities.cHash(sHostname));
    	Utilities.servStrategy strategy=currentSVC.GetStrategy();
    	int cacheSize=currentSVC.GetCacheSize();
		DO = new diskOperation();
		DO.load_lookup_table();
		
		if(strategy == Utilities.servStrategy.FIFO)
			CacheStrategy = IKVServer.CacheStrategy.FIFO;
		else if(strategy == Utilities.servStrategy.LRU)
			CacheStrategy = IKVServer.CacheStrategy.LRU;
		else if(strategy == Utilities.servStrategy.LFU)
			CacheStrategy = IKVServer.CacheStrategy.LFU;
		else
			CacheStrategy = IKVServer.CacheStrategy.None;
		cache = new Cache(CacheStrategy, cacheSize);
    	
		nPort=currentSVC.GetPort();
		
		PrintWriter writer = new PrintWriter("the-file-name.txt", "UTF-8");
		writer.println("The first line");
		writer.println("The second line");
		writer.close();
        serverSocket = new ServerSocket(nPort);
        nPort=serverSocket.getLocalPort();
        
    	return true;

    	
    }
    
    
	public void run(){
        try
        {
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
        }catch(Exception e)
        {
        	
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
    	ServerConfiguration tempSV=SVCs.FindNextHigher(myutilities.cHash(sHostname));
    	//send to tempSV
    	String[] hashRange=new String[2];
    	hashRange[0]=SVCs.FindServerByServerNameHash(myutilities.cHash(sHostname)).GetLower();
    	hashRange[1]=SVCs.FindServerByServerNameHash(myutilities.cHash(sHostname)).GetUpper();
    	moveData(hashRange,tempSV.GetHashValue());

    	
    	
    }
    
    public void remove()
    {

    	try {
        	ServerConfiguration tempSV=SVCs.FindNextHigher(myutilities.cHash(sHostname));
        	//send to tempSV
        	String[] hashRange=new String[2];
        	hashRange[0]=currentSVC.GetLower();
        	hashRange[1]=currentSVC.GetUpper();
			moveData(hashRange,tempSV.GetHashValue());
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

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
    	//ServerKVStore.MoveDataStart();
    	// find all key value pairs fall into this range
    	String lowerbound=hashRange[0];
    	String upperbound=hashRange[1];
    	ArrayList<KeyValuePair> ListToMove=DO.get_subset(lowerbound, upperbound);
    	
    	for(KeyValuePair currentPair:ListToMove)
    	{
    		ServerKVStore.putNoCache(currentPair.getKey(), currentPair.getValue());
    	}
    	
    	for(KeyValuePair currentPair:ListToMove)
    	{
    		DO.put(currentPair.getKey(), "");
    	}
    	//ServerKVStore.MoveDataEnd();
		return true;
	}
    
    @Override
    public void update() throws Exception {
    	//Update the metadata repository of this server
		ServerConfigurations res=new ServerConfigurations();

		List<String> childrenList=zk.getChildren("/servers/", false);
		for(String child: childrenList)
		{
			String mypath="/servers/"+child;
			byte[] temp=zk.getData(mypath, false, zk.exists(mypath, false));
			ServerConfiguration svc= myutilities.ServerConfigByteArrayToSerializable(temp);
			if(svc.GetName().equals(sHostname))
			{
				//ServerMD5Hash=svc.GetHashValue();
				//currentSVC=svc;
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
    		return myutilities.cHash(sHostname).equals(SVCs.FindServerForKey(sHash).GetHashValue());
    }
    
    public boolean IsLocked()
    {
    	return WriteLockFlag;
    }
    
    public boolean IsAcceptingRequest()
    {
    	return HandleClientRequest;
    }
    
    public void ReceivingData(boolean receivingData) throws Exception
    {

    	bReceivingData = receivingData;
    	if(!receivingData && bShouldBeRemoved)
    	{
    		//Move data to next server. Do whatever
    		shutDown();
    	}
    }
    
    public void process(WatchedEvent event) throws Exception, InterruptedException {
  	  String path = event.getPath();
  	  byte[] znodeVal;
       /*if (event.getState() == KeeperState.SyncConnected) {
          connectedSignal.countDown();
       }
       */
       
       if(event.getType() == Event.EventType.None)
       {
      	 //we are being told that the state of the connection has changed
      	 switch(event.getState())
      	 {
      	 	case SyncConnected:
      	 		break;
      	 	case Expired:
      	 		break;
      	 }m2/src/app_kvServer/KVServer.java
       }
       else
       {
      		 //Something has changed on the node
      			 if(path.contains(sHostname))
      			 {
      				 znodeVal=zk.getData(path, true,zk.exists(path, true));
      				 Utilities.servStatus status = myutilities.ServerConfigByteArrayToSerializable(znodeVal).GetStatus();
      				 switch(status)
      				 {
      				 	case adding:
      				 		currentSVC.SetStatus(Utilities.servStatus.added);
      				 		currentSVC.SetPort(nPort);
      				 		zk.setData(path, myutilities.ServerConfigSerializableToByteArray(currentSVC), -1);
      				 		new Thread(this).start();
      				 		break;
      				 	case starting:
      				 		this.start();
      				 		currentSVC.SetStatus(Utilities.servStatus.started);
      				 		zk.setData(path, myutilities.ServerConfigSerializableToByteArray(currentSVC), -1);
      				 		break;
      				 	case stopping:
      				 		this.stop();
      				 		currentSVC.SetStatus(Utilities.servStatus.stopped);
      				 		zk.setData(path, myutilities.ServerConfigSerializableToByteArray(currentSVC), -1);
      				 		break;
      				 	case removing:
      				 		this.lockWrite();
      				 		this.remove();
      				 		this.unlockWrite();
      				 		this.close();
      				 		currentSVC.SetStatus(Utilities.servStatus.removed);
      				 		zk.setData(path, myutilities.ServerConfigSerializableToByteArray(currentSVC), -1);
      				 		break;
      				 	case exiting:
      				 		this.close();
      				 		currentSVC.SetStatus(Utilities.servStatus.exited);
      				 		zk.setData(path, myutilities.ServerConfigSerializableToByteArray(currentSVC), -1);
      				 		break;
      				 	case sending:
      				 		//move data to one before
      				 		this.lockWrite();
      				 		ServerConfiguration tempSVC=myutilities.ServerConfigByteArrayToSerializable(znodeVal);
      				 		String[] tempRange=new String[2];
      				 		tempRange[0]=currentSVC.GetLower();
      				 		tempRange[1]=tempSVC.GetLower();
      				 		update();
      				 		ServerConfiguration targetHost=SVCs.FindOneBefore(myutilities.cHash(sHostname));
      				 		this.moveData(tempRange, targetHost.GetHashValue());
      				 		currentSVC=tempSVC;
      				 		this.unlockWrite();
      				 		zk.setData(path, myutilities.ServerConfigSerializableToByteArray(currentSVC), -1);
      				 		break;
      				 	case adding_starting:
      				 		this.initKVServer();
      				 		this.run();
      				 		this.start();
      				 		currentSVC.SetStatus(Utilities.servStatus.started);
      				 		zk.setData(path, myutilities.ServerConfigSerializableToByteArray(currentSVC), -1);
      				 		break;
      				 	default:
      				 		System.out.println("KVServer process invalid: "+status.toString());
      				 		
      				 }
      			 }
      			 else
      			 {
      				 update();
      			 }
       }
    }
}
