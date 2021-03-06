package app_kvServer;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.net.BindException;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.SortedSet;
import java.util.concurrent.locks.ReentrantLock;

import logger.LogSetup;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.Watcher.*;

import client.KVStore;

import Utilities.Utilities;
import Utilities.Utilities.servStrategy;
import app_kvECS.ZKConnection;
import app_kvServer.IKVServer.CacheStrategy;

public class KVServer extends Thread implements IKVServer, Runnable, Watcher {

	/**
	 * Start KV Server with selected name
	 * 
	 * @param name
	 *            unique name of server
	 * @param zkHostname
	 *            hostname where zookeeper is running
	 * @param zkPort
	 *            port where zookeeper is running
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
	private HashMap<String,String> metadataMap;
	private boolean WriteLockFlag;
	private boolean HandleClientRequest;
	private ZooKeeper zk;

	private Utilities myutilities;
	// private String ServerMD5Hash;
	private boolean bReceivingData;
	private boolean bShouldBeRemoved;
	
	private String ServerPath;
	private String MetaDataPath;
	private String ServerStatusPath;
	private String ServerStrategyPath;
	private String ServerSizePath;
	//private String ServerReplicaPath;
	private String LowerBound;
	private String UpperBound;
	private String HashedName;
	private String CurrentStatus;
	
	private String HashReplica1;
	private String HashReplica2;
	//replicaDO;
	private String replicaStorge;
	private diskOperation replicaDO;
	private HeartBeat HB;
	public KVServer(String name, String zkHostname, int zkPort)
			throws Exception {
		sHostname = name;
		sZKHostname = zkHostname;
		nZKPort = zkPort;

		replicaStorge=sHostname+"_replica";
		HandleClientRequest = false;
		WriteLockFlag = false;
		myutilities = new Utilities();
		HashedName=myutilities.cHash(sHostname);
		bReceivingData = false;
		bShouldBeRemoved = false;

		ServerPath = "/servers/"+sHostname;
		MetaDataPath="/servers/metadata";
		ServerStatusPath=ServerPath+"/status";
		ServerStrategyPath=ServerPath+"/strategy";
		ServerSizePath=ServerPath+"/size";
		
		HashReplica1="";
		HashReplica2="";
		
		zk = new ZooKeeper(sZKHostname + ":" + nZKPort, 5000, this);
		setWatch();
		HB = new HeartBeat(this);
		HB.start();
	}

	void setWatch() throws KeeperException, InterruptedException
	{

		zk.exists(MetaDataPath, true);
		return;
	}

	@Override
	public int getPort() {
		return nPort;
	}

	@Override
	public String getHostname() {
		return sHostname;
	}

	@Override
	public CacheStrategy getCacheStrategy() {
		return CacheStrategy;
	}

	@Override
	public int getCacheSize() {
		return nCacheSize;
	}

	@Override
	public boolean inStorage(String key) {
		return DO.inStorage(key);
	}

	@Override
	public boolean inCache(String key) {
		return (CacheStrategy != IKVServer.CacheStrategy.None)
				&& cache.inCache(key);
	}

	@Override
	public String getKV(String key) throws Exception {
		if (inCache(key)) {
			return cache.get(key);
		} else if (inStorage(key)) {
			String sVal = DO.get(key);
			cache.put(key, sVal);
			return sVal;
		}
		else if(replicaDO.inStorage(key))
		{
			return replicaDO.get(key);
		}
		return "";
	}

	@Override
	public void putKV(String key, String value) throws Exception {
		DO.put(key, value);
		cache.put(key, value);
		//sent to my replica
		putKVToReplica(key,value,HashReplica1);
		putKVToReplica(key,value,HashReplica2);
	}

	@Override
	public void clearCache() {
		cache.clearCache();
	}

	@Override
	public void clearStorage() {
		DO.clearStorage();
	}

	@Override
	public void kill() {
		// TODO Auto-generated method stub
		running = false;
		try {
			serverSocket.close();
		} catch (IOException e) {
			logger.error(
					"Error! " + "Unable to close socket on port: " + nPort, e);
		}
	}

	@Override
	public void close() {
		// TODO Auto-generated method stub
		running = false;
		try {
			serverSocket.close();
		} catch (IOException e) {
			logger.error(
					"Error! " + "Unable to close socket on port: " + nPort, e);
		}
	}

	public boolean isRunning() {
		return this.running;
	}

	public static void main(String[] args) throws Exception {
		try {
			System.out.println("SSH connection complete");
			new LogSetup("logs/server.log", Level.ALL);
			if (args.length != 3) {
				System.out.println("Error! Invalid number of arguments!");
				System.out
						.println("Usage: Server <port> <cache_size> <cache_strategy>!");
			} else {
				String sName = args[0];
				String sZKName = args[1];
				int zkPort = Integer.parseInt(args[2]);
				new KVServer(sName, sZKName, zkPort).start();



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
	public boolean initKVServer() throws Exception {

	
		// call update to update meta data map
		update();
		
		//get cache strategy
		byte[] res=zk.getData(ServerStrategyPath, false, zk.exists(ServerStrategyPath, false));
	
		String cache_strategy=new String(res);

		if(cache_strategy.equals("FIFO"))
		{
			CacheStrategy = IKVServer.CacheStrategy.FIFO;
		}
		else if(cache_strategy.equals("LRU"))
		{
			CacheStrategy = IKVServer.CacheStrategy.LRU;
		}
		else if(cache_strategy.equals("LFU"))
		{
			CacheStrategy = IKVServer.CacheStrategy.LFU;
		}
		else
		{
			CacheStrategy = IKVServer.CacheStrategy.None;
		}

		//get cache size
		res=zk.getData(ServerSizePath, false, zk.exists(ServerSizePath, false));

		nCacheSize=Integer.parseInt(new String(res));

		DO = new diskOperation(sHostname);

		DO.load_lookup_table();
		
		//load from replica file
		replicaDO = new diskOperation(replicaStorge);
		replicaDO.load_lookup_table();
		
		cache = new Cache(CacheStrategy, nCacheSize);

		
		String ServerInfo=metadataMap.get(HashedName);
		String[] ServerInfos=ServerInfo.trim().split("\\s+");

		nPort=Integer.parseInt(ServerInfos[1]);
		LowerBound=ServerInfos[2];
		UpperBound=ServerInfos[3];

		serverSocket = new ServerSocket(nPort);

		String ReturnStatus="added";
		CurrentStatus="added";
		zk.setData(ServerStatusPath, ReturnStatus.getBytes(), -1);

		return true;

	}

	public void run() {
		try {
			running = initKVServer();
			if (serverSocket != null) {
				while (isRunning()) {
					try {
						Socket client = serverSocket.accept();
						ClientConnection connection = new ClientConnection(
								client, this);
						new Thread(connection).start();

						logger.info("Connected to "
								+ client.getInetAddress().getHostName()
								+ " on port " + client.getPort());
					} catch (IOException e) {
						logger.error("Error! "
								+ "Unable to establish connection. \n", e);
					}
				}
			}
		} catch (Exception e) {

		}
		logger.info("Server stopped.");
	}

	@Override
	public void start_request() {
		// TODO
		// Starts the KVServer, all client requests and all ECS requests are
		// processed.
		HandleClientRequest = true;
	}

	@Override
	public void stop_request() {
		// TODO
		// Stops the KVServer, all client requests are rejected and only ECS
		// requests are processed.
		HandleClientRequest = false;
	}


	//need to fix this
	public void remove() {

		try {
			String TheRightOne=myutilities.FindNextOne(metadataMap, HashedName);
					
			String ServerInfo=metadataMap.get(TheRightOne);
			String[] ServerInfos=ServerInfo.trim().split("\\s+");
			
			String TargetHostname=ServerInfos[0];
			int TargetPort=Integer.parseInt(ServerInfos[1]);
			

			ServerKVStore = new KVStore(TargetHostname, TargetPort);
			ServerKVStore.connect();

			Set<String> mykeys=DO.getKeySet();
			for(String key:mykeys)
			{

				ServerKVStore.putNoCache(key, DO.get(key));
				DO.put(key,"");
				cache.put(key, "");
				
			}

			// ServerKVStore.MoveDataEnd();
			ServerKVStore.disconnect();
			
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	@Override
	public void lockWrite() {
		// TODO
		// Lock the KVServer for write operations.
		// lock current server for write operations
		WriteLockFlag = true;
	}

	@Override
	public void unlockWrite() {
		// TODO
		// Unlock the KVServer for write operations.
		WriteLockFlag = false;
	}

	@Override
	public boolean moveData(String targetName)
			throws Exception {
		// TODO
		// Transfer a subset (range) of the KVServer's data to another KVServer
		// (reallocation before removing this server or adding a new KVServer to
		// the ring);
		// send a notification to the ECS, if data transfer is completed.
		// create KVStore object here to take connections from client
		// use targetName to find appropriate address, port
		String ServerInfo=metadataMap.get(targetName);
		String[] ServerInfos=ServerInfo.trim().split("\\s+");
		
		String TargetHostname=ServerInfos[0];
		int TargetPort=Integer.parseInt(ServerInfos[1]);
		

		ServerKVStore = new KVStore(TargetHostname, TargetPort);
		ServerKVStore.connect();

		
		Set<String> mykeys=DO.getKeySet();
		for(String key:mykeys)
		{
			if(IsResponsible(myutilities.cHash(key),ServerInfos[2], ServerInfos[3]))
			{
				ServerKVStore.putNoCache(key, DO.get(key));
				DO.put(key,"");
				cache.put(key, "");
			}
			/*
			String tempKeyHash=myutilities.cHash(key);
			// if not in bound then, send remove
			if(LowerBound.compareTo(UpperBound)<0)
			{
				//lower < upper
				if(tempKeyHash.compareTo(LowerBound)<=0 || UpperBound.compareTo(tempKeyHash)<0)
				{
					//current <= lower or temp > upper out of bound
					ServerKVStore.putNoCache(key, DO.get(key));
					DO.put(key,"");
					cache.put(key, "");
				}
				
			}
			else if(UpperBound.compareTo(LowerBound)<0)
			{
				//lower>upper
				if(tempKeyHash.compareTo(LowerBound)<=0 && UpperBound.compareTo(tempKeyHash)<0)
				{
					
					ServerKVStore.putNoCache(key, DO.get(key));
					DO.put(key,"");
					cache.put(key, "");
				}
			}
			else
			{
				ServerKVStore.putNoCache(key, DO.get(key));
				DO.put(key,"");
				cache.put(key, "");
			}
			*/
		}

		// ServerKVStore.MoveDataEnd();
		ServerKVStore.disconnect();
		return true;
	}

	
	@Override
	public void update() throws Exception {
		// Update the metadata repository of this server

		byte[] res=zk.getData(MetaDataPath, true, zk.exists(MetaDataPath, true));

		metadataMap=myutilities.DeserializeByteArrayToHashMap(res);
		String ServerInfo=metadataMap.get(HashedName);
		String[] ServerInfos=ServerInfo.trim().split("\\s+");
		LowerBound=ServerInfos[2];
		UpperBound=ServerInfos[3];

	}

	@Override
	public void putNoCache(String key, String value) throws IOException,
			NoSuchAlgorithmException {
		DO.put(key, value);
		if(cache.inCache(key))
		{
			cache.put(key, value);
		}
	}

	public boolean IsLocked() {
		return WriteLockFlag;
	}

	public boolean IsAcceptingRequest() {
		return HandleClientRequest;
	}

	//need to fix this
	public void ReceivingData(boolean receivingData) throws Exception {

		bReceivingData = receivingData;
		if (!receivingData && bShouldBeRemoved) {
			// Move data to next server. Do whatever
			
		}
	}

	public String getResponsible(String key) throws UnsupportedEncodingException, NoSuchAlgorithmException
	{
		String res="";
		String HashedKey=myutilities.cHash(key);
		Set<String>Servers=metadataMap.keySet();
		for(String temp:Servers)
		{
			String ServerInfo=metadataMap.get(temp);
			String[] ServerInfos=ServerInfo.trim().split("\\s+");
			String currentLower=ServerInfos[2];
			String currentUpper=ServerInfos[3];
			if(IsResponsible(HashedKey, currentLower, currentUpper))
				return ServerInfos[0] + " " + ServerInfos[1]; 
		}
		return res;
	}
	public void process(WatchedEvent event) {
		try {
			if (event.getType() == Event.EventType.None) {
				// we are being told that the state of the connection has
				// changed
				switch (event.getState()) {
				case SyncConnected:
					break;
				case Expired:
					break;
				}
			} else {
				// Something has changed on the node
				update();
				if (true) {
					//check its own node
					byte[] res = zk.getData(ServerStatusPath, false, zk.exists(ServerStatusPath, false));
					String status=new String(res);
					if(status.equals("adding"))
					{
						CurrentStatus="added";
						zk.setData(ServerStatusPath, CurrentStatus.getBytes(), -1);
					}
					else if(status.equals("starting"))
					{
						this.start_request();
						CurrentStatus="started";
						zk.setData(ServerStatusPath, CurrentStatus.getBytes(), -1);
					}
					else if(status.equals("stopping"))
					{
						this.stop_request();
						CurrentStatus="added";
						zk.setData(ServerStatusPath, CurrentStatus.getBytes(), -1);
					}
					else if(status.equals("removing"))
					{
						//need to fix
						//CurrentStatus="removed";
						//zk.setData(ServerStatusPath, CurrentStatus.getBytes(), -1);
						this.lockWrite();
						this.remove();
						this.unlockWrite();
						this.close();
						CurrentStatus="removed";
						zk.setData(ServerStatusPath, CurrentStatus.getBytes(), -1);
					}
					else if(status.equals("exiting"))
					{
						this.close();
						CurrentStatus="exited";
						zk.setData(ServerStatusPath, CurrentStatus.getBytes(), -1);
					}
					else if(status.equals("sending"))
					{
						//need to fix
						// older lower bound to new lower bound
						this.lockWrite();
						String TargetHost=myutilities.FindBeforeOne(metadataMap, HashedName);					
						moveData(TargetHost);
						this.unlockWrite();
						zk.setData(ServerStatusPath, CurrentStatus.getBytes(), -1);
					}
					else if(status.equals("settingReplica"))
					{
						replicaDO.clearStorage();
						GetReplicaStatus();
						zk.setData(ServerStatusPath, CurrentStatus.getBytes(), -1);
					}
					else if(status.equals("sendingReplica"))
					{
						putKVToReplicaServerToServer(HashReplica1);
						putKVToReplicaServerToServer(HashReplica2);
						zk.setData(ServerStatusPath, CurrentStatus.getBytes(), -1);
					}

				}
			} 
		}
		catch (Exception e) {
		}
	}
	
	public void GetReplicaStatus() throws KeeperException, InterruptedException
	{
		HashReplica1=new String( zk.getData(ServerPath+"/replica1", false, zk.exists(ServerPath+"/replica1", false)));
		HashReplica2=new String( zk.getData(ServerPath+"/replica2", false, zk.exists(ServerPath+"/replica2", false)));
	}
	public boolean IsResponsible(String HashedKey, String currentLower, String currentUpper)
	{
		if(metadataMap.size() == 1)
			return true;
		if(currentLower.compareTo(currentUpper)>0)
		{
			String zero="";
			String FF="";
			for(int i=0;i<32;i++)
			{
				zero+="0";
				FF+="f";
			}
			if(HashedKey.compareTo(currentLower)>0 && FF.compareTo(HashedKey)>0)
				return true;
			else if(HashedKey.compareTo(zero)>0 && currentUpper.compareTo(HashedKey)>0)
				return true;		
		}
		else
		{
			if(HashedKey.compareTo(currentLower)>0 && currentUpper.compareTo(HashedKey)>0)
				return true;
		}
		return false;
	}
	
	public boolean IsReplicaResponsible(String Hashedkey)
	{
		if(metadataMap.size() <= 3)
			return true;
		else
		{
			String closer = myutilities.FindBeforeOne(metadataMap, HashedName);
			String middle = myutilities.FindBeforeOne(metadataMap, closer);
			String farther = myutilities.FindBeforeOne(metadataMap, middle);
			return IsResponsible(Hashedkey, farther, closer);
		}
	}
	//need to fix
	public void putKVToReplica(String key,String value,String replica) throws Exception
	{
		if(replica.equals(""))
			return;

		String ServerInfo=metadataMap.get(replica);
		
		String[] ServerInfos=ServerInfo.trim().split("\\s+");
		
		String DestinationAddress=ServerInfos[0];
		
		int DestinationPort=Integer.parseInt(ServerInfos[1]);
		
		ServerKVStore = new KVStore(DestinationAddress, DestinationPort);
		ServerKVStore.connect();
		ServerKVStore.putNoCache(key, value);
		ServerKVStore.disconnect();

	}
	public void putKVToReplicaServerToServer(String replica) throws Exception
	{
		if(replica.equals(""))
			return;
		Set<String> mykeys=DO.getKeySet();
		
		String ServerInfo=metadataMap.get(replica);
		
		String[] ServerInfos=ServerInfo.trim().split("\\s+");
		
		String DestinationAddress=ServerInfos[0];
		
		int DestinationPort=Integer.parseInt(ServerInfos[1]);
		
		ServerKVStore = new KVStore(DestinationAddress, DestinationPort);
		ServerKVStore.connect();
		for(String key:mykeys)
		{
			ServerKVStore.putNoCache(key, getKV(key));
		}
		ServerKVStore.disconnect();

	}
	
	public String GetLowerBound()
	{
		return LowerBound;
	}
	
	public String GetUpperBound()
	{
		return UpperBound;
	}
	
	void HeartBeat() throws KeeperException, InterruptedException
	{
		String currentTimeStamp=new String(zk.getData("/servers/"+sHostname+"/crash", false, zk.exists("/servers/"+sHostname+"/crash", false)));
		int incremented=Integer.parseInt(currentTimeStamp)+1;
		String newTimeStamp=incremented+"";
		zk.setData("/servers/"+sHostname+"/crash", newTimeStamp.getBytes(), -1);
	}
}
