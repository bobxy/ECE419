package app_kvECS;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Collection;
import java.util.Collections;
import java.util.StringTokenizer;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.*;

import common.ServerConfiguration;

import Utilities.Utilities;


import ecs.ECSNode;
import ecs.IECSNode;

public class ECSClient implements IECSClient {
	
	private static final String PROMPT = "ECSClient> ";
	private static final String START = "start";
	private static final String STOP = "stop";
	private static final String SHUTDOWN = "shutdown";
	private static final String HELP = "help";
	private static final String ADDNODES = "addnodes";
	private static final String ADDNODE = "addnode";
	private static final String REMOVENODE = "removenode";
	private static final String GETNODES = "getnodes";
	private static final String GETNODE = "getnode";
	
	private ZKConnection zkC;
	private ZooKeeper zk;
	private Collection<IECSNode> ECSNodeList;
	private Collection<IECSNode> activeECSNodeList;
	private Utilities uti;

	public ECSClient(){
		//establish zookeeper connection and return zookeeper object
		System.out.println("11");
		zkC = new ZKConnection();
		System.out.println("12");
		try{
			System.out.println("13");
			zk = zkC.connect("127.0.0.1:6666");
			
		}catch (Exception e)
		{
			System.out.println("cannot connect to zookeeper!");
		}
		
		//create zookeeper nodes for server and fct
		try {
			//zk.delete("/servers", zk.exists("/servers", false).getVersion());
			//zk.delete("/actions",zk.exists("/actions", false).getVersion());
			zk.create("/servers", null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
			zk.create("/actions", null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		} catch (KeeperException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
		uti = new Utilities();
		ECSNodeList = new ArrayList<IECSNode>();
		activeECSNodeList = new ArrayList<IECSNode>();
	}

    @Override
    public boolean start() {
        // TODO
    	String path;
    	
    	for (IECSNode e:activeECSNodeList){
    		 
    		path = "/servers/" + e.getNodeName();
    		
    		
    		
    	}
        return false;
    }

    @Override
    public boolean stop() {
        // TODO
        return false;
    }

    @Override
    public boolean shutdown() {
        // TODO
        return false;
    }

    @Override
    public IECSNode addNode(String cacheStrategy, int cacheSize) throws NoSuchAlgorithmException, KeeperException, InterruptedException, IOException {
    	
    	//calls setupNodes
    	Collection<IECSNode> newNodes = new ArrayList<IECSNode>();
    	
    	newNodes = setupNodes(1,cacheStrategy,cacheSize);
    	
    	//do ssh
    	createScript(newNodes);
    	
    	Process proc;
    	String script = "script.sh";
    	
    	Runtime run = Runtime.getRuntime();
    	try{
    		proc = run.exec("chmod u+x /m2/" + script);
    	}catch (IOException e){
    		e.printStackTrace();
    	}
    	
    	
    	//String path = "/servers/" + ((ArrayList <IECSNode>)newNodes).get(0).getNodeHost() + "/status";
    	
    	//byte[] data = "SERVER_START".getBytes();
    			
    	//zk.setData(path, data, zk.exists(path, false).getVersion());
    			
        return ((ArrayList <IECSNode>)newNodes).get(0);
    }

    @Override
    public Collection<IECSNode> addNodes(int count, String cacheStrategy, int cacheSize) throws NoSuchAlgorithmException, KeeperException, InterruptedException, IOException {
        
    	//calls setupNodes
    	Collection<IECSNode> newNodes = new ArrayList<IECSNode>();
    	
    	newNodes = setupNodes(count,cacheStrategy,cacheSize);
    	
    	//do ssh
    	createScript(newNodes);
    	
    	Process proc;
    	String script = "script.sh";
    	
    	Runtime run = Runtime.getRuntime();
    	try{
    		System.out.println("perform ssh");
    		
    		proc = run.exec("chmod u+x ./" + script);
    	}catch (IOException e){
    		e.printStackTrace();
    	}
    	
    	System.out.println("ssh done");
    		 	
    	//calls awaitNodes
    	
    	int timeout = 10000; //set timer in milliseconds
    	
    	boolean rdy = false;
    	
    	try {
			rdy = awaitNodes(count,timeout);
		} catch (Exception e) {
			e.printStackTrace();
		}
    	
    	if (rdy)
    		return newNodes;
    	
    	return null;
    }

    @Override
    public Collection<IECSNode> setupNodes(int count, String cacheStrategy, int cacheSize) throws UnsupportedEncodingException, NoSuchAlgorithmException, KeeperException, InterruptedException {
    	
    	if (count == 0){
    		System.out.println("No server added");
    		return activeECSNodeList;
    	}
    	
    	if (ECSNodeList.size() == 0){
    		System.out.println("No more available server; add failed");
    		return activeECSNodeList;
    	}
    	
    	IECSNode newNode;
    	String hash;
    	Collection<IECSNode> addedList = new ArrayList<IECSNode>();
        
    	for (int i=1; i<= count; i++)
    	{
    		//random pick from list of available nodes
    		int idx = ThreadLocalRandom.current().nextInt(0, ECSNodeList.size());
    		
    		newNode =  ((ArrayList<IECSNode>) ECSNodeList).get(idx);
    		
    		//remove selected from list of available
    		((ArrayList<IECSNode>) ECSNodeList).remove(idx);
    		 		
    		//perform consistent hashing
			hash = uti.cHash(newNode.getNodeHost() + ":" + Integer.toString(newNode.getNodePort()));
	
    		newNode.setNodeHashValue(hash);
    		
    		//add to list for all active servers
    		activeECSNodeList.add(newNode);
    		
    		//add to list for newly added servers
    		addedList.add(newNode);
    		   		
    		if (ECSNodeList.size() == 0){
    			System.out.printf("Only able to add %d servers%n",i);
    			break;
    		}	
    	}
    	
    	
    	//update hash ring
    	
    	updateHRange();
    	
    	updateMetaData();
   
        return addedList;
    }

    @Override
    public boolean awaitNodes(int count, int timeout) throws Exception {
       
    	long startTime = System.currentTimeMillis();
    	long elapsedTime = 0l;
    	
    	
    	while ((int)elapsedTime < timeout){
    		
    		int rdyServCount=0;
    		
    		for (IECSNode node:activeECSNodeList){
    			
    			String path = "/servers/" + node.getNodeName();
    			
    			byte[] temp = zk.getData(path, false, null);
    			    			
    			//String status = new String (temp,"UTF-8");
    			
    			ServerConfiguration sc = uti.ServerConfigByteArrayToSerializable(temp);
    			
    			if (!sc.GetStatus().equals("uninitialized")){
    				rdyServCount++;
    			}
    		}
    		
    		if (rdyServCount == activeECSNodeList.size()){
    			return true;
    		}
    		
    		elapsedTime = (new Date()).getTime() - startTime;
    	}
    	
        return false;
    }

    @Override
    public boolean removeNodes(Collection<String> nodeNames) {
        
    	for (IECSNode node : activeECSNodeList){
    		;
    	}
    	
        return false;
    }

    @Override
    public Map<String, IECSNode> getNodes() {
        // TODO
        return null;
    }

    @Override
    public IECSNode getNodeByKey(String Key) {
        // TODO
        return null;
    }
    
    public void run(){
    	try
    	{
    		while(true)
    		{
    			System.out.print(PROMPT);
    			BufferedReader stdin = new BufferedReader(new InputStreamReader(System.in));
    			String cmdLine = stdin.readLine();
    			String[] sElements = Parse(cmdLine);
    			if(sElements != null)
    			{
    				String sAction = sElements[0];
    				if(sAction.equals(HELP))
    					help();
    				else if(sAction.equals(START))
    				{
    					if (start())
    					{
    						System.out.println("All servers started");
    					}
    					else
    						System.out.println("Can not start servers");
    				}
    				else if(sAction.equals(STOP))
    				{
    					if (stop())
    					{
    						System.out.println("All servers stopped");
    					}
    					else
    						System.out.println("Can not stop servers");
    				}
    				else if(sAction.equals(ADDNODES))
    				{
    					Collection<IECSNode> ECSNodes_group;
    					
    					int numNodes = Integer.parseInt(sElements[1]);
    					String cacheStrgy = sElements[2];
    					int cacheSze = Integer.parseInt(sElements[3]);
    				
    					//write set ECSNodes function
    					ECSNodes_group = addNodes(numNodes,cacheStrgy,cacheSze);
    					
    					if (ECSNodes_group != null)
    						System.out.println("nodes added successfully");
    					else
    						System.out.println("Add nodes failed");
    					//what to do with it
    				}
    				else if(sAction.equals(ADDNODE))
    				{
    					IECSNode nodeServer;
    					String cacheStrgy = sElements[1];
    					int cacheSze = Integer.parseInt(sElements[2]);
    					
    					nodeServer = addNode(cacheStrgy,cacheSze);
    					
    					//what to do with it
    				}
    				else if(sAction.equals(REMOVENODE))
    				{
    					Collection<String> nodeNames = new ArrayList<String>();
    					
    					for (int i=1; i<sElements.length; i++)
    					{
    						nodeNames.add(sElements[i]);
    					}
    					
    					boolean removed = removeNodes(nodeNames);
    					
    					if (removed)
    						System.out.println("Successfully removed");
    					else
    						System.out.println("Can not remove nodes");
    						
    					//what to do with it?
    				}
    				else if (sAction.equals(GETNODES))
    				{
    					Map <String, IECSNode> nodeMap = getNodes();
    					//what to do with it?
    				}
    				else if (sAction.equals(GETNODE))
    				{
    					String sKey = sElements[1];
    					
    					IECSNode nodeServer = getNodeByKey(sKey);
    					
    					//what to do with it?
    				}
    				else if(sAction.equals(SHUTDOWN))
    				{
    					if (shutdown())
    					{
    						System.out.println("Shutdown successful");
    					}
    					else
    						System.out.println("Can not shutdown system");
    				}
    			}
    			else
    				System.out.println("Error. Type help for instructions.");
    		}
    	}
    	catch (Exception e) {
			System.out.println("Error!");
			e.printStackTrace();
			System.exit(1);
		}
    }
    
    private String[] Parse(String sCommand)
    {
    	try
    	{
	    	sCommand = sCommand.trim();
	    	String[] sElements = sCommand.split(" ");
	    	int nElementCount = sElements.length;
	  
	    	if (sCommand.equals(START) || sCommand.equals(STOP) || sCommand.equals(SHUTDOWN) || sCommand.equals(GETNODES) || sCommand.equals(HELP))
	    	{
	    		String[] ret = {sCommand};
	    		return ret;
	    	}
	    	  	
	    	//Error
	    	if(nElementCount < 2)
	    		return null;
	    	
	    	String sAction = sElements[0];
	    	
	    	//addNodes command
	    	if(sAction.equals(ADDNODES) && nElementCount == 4)
	    	{
	    		String numNodes = sElements[1];
	    		String cacheSze = sElements[2];
	    		String cacheStrgy = sElements[3];
	    		String[] ret = {sAction, numNodes, cacheSze, cacheStrgy};
	    		return ret;
	    	}
	    	
	    	//add single node command
	    	if(sAction.equals(ADDNODE) && nElementCount == 3)
	    	{
	      		String cacheSze = sElements[1];
	    		String cacheStrgy = sElements[2];
	    		String[] ret = {sAction, cacheSze, cacheStrgy};
	    		return ret;
	    	}
	    	
	    	//remove node command
	    	if(sAction.equals(REMOVENODE))
	    	{
	    		return sElements;
	    	}
	    	
	    	//get node command
	    	if (sAction.equals(GETNODE) && nElementCount == 2)
	    	{
	    		String sKey = sElements[1];
	    		String[] ret = {sAction, sKey};
	    		return ret;
	    	}
	  
    	}catch(Exception e) {
			System.out.println("Error when parsing command!");
			e.printStackTrace();
			System.exit(1);
    	}
    	return null;
    }
    
    private void help(){
    	System.out.println("Please follow the following formats for the instructions");
    	System.out.println(ADDNODES + " <number of nodes>"  + " <replacement strategy>" + " <size of cache>");
    	System.out.println(ADDNODE + " <replacement strategy>" + " <size of cache>");
    	System.out.println(REMOVENODE + " <node name 1>" + " <node name 2>" + " ...");
    	System.out.println(GETNODE + " <key>");
    	System.out.println(START);
    	System.out.println(STOP);
    	System.out.println(SHUTDOWN);
    	System.out.println(GETNODES);
    }
    
    public void readConfig(String file) throws IOException{
    	
    	File servConfig = new File(file);
    	FileReader fr = new FileReader(servConfig);
    	BufferedReader br = new BufferedReader(fr);
    	String currentLine;
    	
    	System.out.println("readconfig");
    	while ((currentLine = br.readLine()) != null)
    	{
    		System.out.println(currentLine);
    		
    		StringTokenizer st = new StringTokenizer(currentLine);
    		
    		String servName = st.nextToken();
    		String servAddress = st.nextToken();
    		String port = st.nextToken();
    		
    		int servPort = Integer.parseInt(port);
    		
    		ECSNode servNode = new ECSNode(servName,servAddress,servPort);
    		
    		ECSNodeList.add(servNode);
   		
    	}
    	
    	br.close();
    	fr.close();
    }
    
    //update hash ring
    
    public void updateHRange(){
    	
    	System.out.println(activeECSNodeList);
    	
    	//sort the IECSNodeList according to hashvalue of each server in ascending
    	Collections.sort((ArrayList<IECSNode>)activeECSNodeList, new Comparator<IECSNode>(){
        	public int compare (IECSNode one, IECSNode other) {
        		return one.getNodeHashValue().compareTo(other.getNodeHashValue());
        	}
        });
    	
    	System.out.println("activeList sorted");
    	
    	String upperB;
    	String lowerB;
    	//find the hash range for each server (construct the ring)
    	for (int i=0; i<activeECSNodeList.size(); i++){
    		
    		if (i!=0){
    			
				System.out.println(" none zero element");

    			 upperB = ((ArrayList<IECSNode>)activeECSNodeList).get(i).getNodeHashValue();
    			 lowerB = ((ArrayList<IECSNode>)activeECSNodeList).get(i-1).getNodeHashValue();
    			 
 				System.out.println("ok2");

    		}
    		else{
    				System.out.println("zero element");
    				upperB = ((ArrayList<IECSNode>)activeECSNodeList).get(i).getNodeHashValue();
    				lowerB = ((ArrayList<IECSNode>)activeECSNodeList).get(activeECSNodeList.size()-1).getNodeHashValue();	
    				System.out.println("ok");
    		}
    		
    		((ArrayList<IECSNode>)activeECSNodeList).get(i).setNodeHashRange(lowerB, upperB);
    	}
    }
    
    public void updateMetaData() throws KeeperException, InterruptedException{
    	//create znode and metadata
    	
    	IECSNode servNode;
    	
    	//create zookeeper znode
    	for (int i=0;i<activeECSNodeList.size(); i++){
    		
    		System.out.println("UMD0");
    		servNode = ((ArrayList<IECSNode>) activeECSNodeList).get(i);
    		
    		String servName = servNode.getNodeName();
    		String servAddr = servNode.getNodeHost();
    		int servPort = servNode.getNodePort();
    		String servHashV = servNode.getNodeHashValue();
    		String[] servHashR = servNode.getNodeHashRange();
    		String servStatus = servNode.getNodeStatus();
    		
    		//check if znode already exist and modify
    		String path = "/servers/" + servName;
    		//String addrPath = path + "/addr";
			//String portPath = path + "/port";
			//String hRangePath = path + "/range";
			//String statusPath = path + "/status";
			
			/*byte[] nameData = servName.getBytes();
			byte[] addrData = servAddr.getBytes();
			byte[] portData = servPort.getBytes();
			byte[] hRangeData = (servHashR[0] + servHashR[1]).getBytes();
			byte[] statusData = "none".getBytes();
			*/
    		ServerConfiguration servConfig = new ServerConfiguration(servAddr,servPort,servHashR[1],servHashR[0],servHashV,servStatus);
    		
    		byte[] data;
			
    		try {
				data = uti.ServerConfigSerializableToByteArray(servConfig);
				
				if (zk.exists(path, true) != null){
	    			 
	    			System.out.println("exist0");
	    			//if server already exist, only need to update hash range
	    			//zk.setData(hRangePath, hRangeData, zk.exists(hRangePath, true).getVersion());
	    			zk.setData(path, data, zk.exists(path, true).getVersion());
	    			System.out.println("exist1");
	    		}
	    		
	    		//else doesn't exist create new znode
	    		else{
				
	    			zk.create(path, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
	    			//zk.create(addrPath, addrData, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
	    			//zk.create(portPath, portData, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
	    			//zk.create(hRangePath, hRangeData, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
	    			//zk.create(statusPath, statusData, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
	    			//set watcher on status??
	    		}
	    		System.out.println("UMD1");
			} catch (Exception e) {
				
				e.printStackTrace();
				System.out.println("failed to serialize server configuration");
			}
    			
			System.out.println("UMD2");
    		
    		
    	}
    }
    
    public void createScript(Collection<IECSNode> newNodes) throws IOException{
    	
    	try {
    		
    		File configFile = new File("script.sh");
    		
    		configFile.createNewFile();
    		
			PrintWriter out = new PrintWriter(new FileOutputStream("script.sh",false));
			
			out.println("ssh-keygen");
			
			IECSNode temp;
			
			for (int i=0; i<newNodes.size(); i++){
				
				temp = ((ArrayList<IECSNode>) newNodes).get(i);
				
				out.println("ssh-copy-id " + "\"" + "localhost " + "-p " + Integer.toString(temp.getNodePort()) + "\"");
				
				out.println("ssh -n localhost nohup java -jar ./m2-server.jar " + temp.getNodeName() + " " + "localhost " + "6666 &");
				
			}
			
			out.close();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.out.println("Cannot create ssh script");
		}
    	
    	
    }
    
    /////////////main

    public static void main(String[] args) {
        
    	System.out.println("1");
    	
    	ECSClient cli = new ECSClient();
    	System.out.println("2");
    	try {
    		System.out.println("3" + args[0]);
			cli.readConfig(args[0]);
			System.out.println("4");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			System.out.println("5");
			e.printStackTrace();
			System.out.println("cannot load config file");
		}
    	
    	cli.run();
    }
}
