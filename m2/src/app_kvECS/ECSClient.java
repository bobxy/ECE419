package app_kvECS;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.net.InetAddress;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
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
import common.ServerConfigurations;

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
	private HashMap<String, String> metaData;
	private Utilities uti;
	public static int counter = 0;

	public ECSClient(){
		//establish zookeeper connection and return zookeeper object
		System.out.println("11");
		zkC = new ZKConnection();
		System.out.println("12");
		try{
			System.out.println("13");
			zk = zkC.connect("127.0.0.1:8094");
			
		}catch (Exception e)
		{
			System.out.println("cannot connect to zookeeper!");
		}
		
		//create zookeeper nodes for server and fct
		try {
			zk.create("/servers", null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
			zk.create("/servers/metadata", null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
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
		metaData= new HashMap<String, String>();
	}

    @Override
    public boolean start() throws Exception {
    
    	String statusPath;
    	String status;
    	byte[] statusData;
    	
    	String metaDataPath = "/servers/metadata";
    	byte[] updated_metaData = uti.SerializeHashMapToByteArray(metaData);
    	
    	for (IECSNode servNode:activeECSNodeList){
    		 
    		statusPath = "/servers/" + servNode.getNodeName()+ "/status";
    		
    		statusData = zk.getData(statusPath, false, null);
    		
    		status = statusData.toString();
    		
    		if (status.equals("started") || status.equals("stopped"))
    		{
    				
    			status = "starting";
    			
    			statusData = status.getBytes();
				
    			zk.setData(statusPath, statusData, (zk.exists(statusPath, false).getVersion()));
    		} 
    		
    	}
    	
    	zk.setData(metaDataPath, updated_metaData, (zk.exists(metaDataPath, false).getVersion()));
    	
        return true;
    }

    @Override
    public boolean stop() throws Exception, InterruptedException {
    
    	String statusPath;
    	String status;
    	byte[] statusData;
    	
    	String metaDataPath = "/servers/metadata";
    	byte[] updated_metaData = uti.SerializeHashMapToByteArray(metaData);
    	
    	for (IECSNode servNode:activeECSNodeList){
    		 
    		statusPath = "/servers/" + servNode.getNodeName() + "/status";
    		
    		status = "stopping";
    		
    		statusData = status.getBytes();
				
    		zk.setData(statusPath, statusData, (zk.exists(statusPath, false).getVersion()));
		
    	}
    	
    	zk.setData(metaDataPath, updated_metaData, (zk.exists(metaDataPath, false).getVersion()));
    	
        return true;
    }

    @Override
    public boolean shutdown() throws Exception {
    	
    	String statusPath;
    	String sizePath;
    	String strategyPath;
    	String namePath;
    	String metaDataPath = "/servers/metadata";
    	String status;
    	byte[] statusData;
    	
    	List<String>childNodeNames=zk.getChildren("/servers", false);
    	
    	for (String nodeName: childNodeNames)
    	{
    		
    		if (nodeName.equals("metadata"))
    		{
    			//wait till all servers close connection, then delete metadata
    			continue;
    			
    		}
    		else
    		{
    			statusPath = "/servers/" +nodeName + "/status";
    			sizePath = "/servers/" +nodeName + "/size";
    			strategyPath = "/servers/" +nodeName + "/strategy";
    			namePath = "/servers/" +nodeName;
    			
    			status = "exiting";
    			statusData = status.getBytes();
    			
    			zk.setData(statusPath, statusData, (zk.exists(statusPath, false).getVersion()));
    			
    			while (true)
    			{
    				statusData = zk.getData(statusPath, false, null);
    				
    				status = statusData.toString();
    				
    				if (status.equals("exited"))
    				{
    					zk.setData(statusPath, null, (zk.exists(statusPath, false).getVersion()));
    					zk.setData(sizePath, null, (zk.exists(sizePath, false).getVersion()));
    					zk.setData(strategyPath, null, (zk.exists(strategyPath, false).getVersion()));
    					zk.setData(namePath, null, (zk.exists(namePath, false).getVersion()));
    					
    					zk.delete(statusPath, (zk.exists(statusPath, false).getVersion()));
    					zk.delete(sizePath, (zk.exists(sizePath, false).getVersion()));
    					zk.delete(strategyPath, (zk.exists(strategyPath, false).getVersion()));
    					zk.delete(namePath, (zk.exists(namePath, false).getVersion()));
    					
    					System.out.print(nodeName + " exited");
    					break;
    				}
    			}
    		}	
    				
    	}
    	
    	zk.setData(metaDataPath, null, (zk.exists(metaDataPath, false).getVersion()));
    	zk.delete(metaDataPath, zk.exists(metaDataPath, false).getVersion());
    	
    	zk.setData("/servers", null, zk.exists("/servers", false).getVersion());
		zk.delete("/servers", zk.exists("/servers", false).getVersion());
    	
		zk.close();
		zk = null;
		
		return true;
    }

    @Override
    public IECSNode addNode(String cacheStrategy, int cacheSize) throws Exception {
    	
    	IECSNode servNode = ((ArrayList<IECSNode>)setupNodes(1,cacheStrategy,cacheSize)).get(0);
    	
    	String script = createScript(servNode);
    	
    	Runtime run = Runtime.getRuntime();
    	
    	Process proc = run.exec(script);
    	
    	proc.waitFor();
    	
    	for (int i=0; i<activeECSNodeList.size(); i++){
			
			if(servNode == ((ArrayList<IECSNode>)activeECSNodeList).get(i)){
				
				int nextNodeIdx = i+1;
				
				IECSNode nextNode;
				
				if (nextNodeIdx == activeECSNodeList.size()){
					nextNode = ((ArrayList<IECSNode>)activeECSNodeList).get(0);
				}
				
				else{
					nextNode = ((ArrayList<IECSNode>)activeECSNodeList).get(nextNodeIdx);
				}
				
				String statusPath = "/servers/" + nextNode.getNodeName() + "/status";
				
				String status = "sending";
				
				byte[] statusData = status.getBytes();
				
				zk.setData(statusPath, statusData, zk.exists(statusPath, false).getVersion());
				
				while (true){
					
					statusData = zk.getData(statusPath, false, null);
					
					status = statusData.toString();
					
					if(!(status.equals("sending"))){
						System.out.println(nextNode.getNodeName() + " finished sending");
						break;
					}
				}
				break;
			}
		}
    	
    	return servNode;
    }

    @Override
    public Collection<IECSNode> addNodes(int count, String cacheStrategy, int cacheSize) throws Exception {
       
    	Collection<IECSNode> newNodes = new ArrayList<IECSNode>();
    	
    	for (int i=0; i<count; i++){
    		newNodes.add(addNode(cacheStrategy, cacheSize));
    	}
    	
    	int timeout = 10000; //set timer in milliseconds
    	
		if(awaitNodes(count,timeout)){
			return newNodes;
		}
		else 
			return null;
		
    }

    @Override
    public Collection<IECSNode> setupNodes(int count, String cacheStrategy, int cacheSize) throws Exception {
    	
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
			hash = uti.cHash(newNode.getNodeName());
	
    		newNode.setNodeHashValue(hash);
    		
    		newNode.setNodeCacheSize(cacheSize);
    		
    		newNode.setNodeStrategy(cacheStrategy);
    		
    		//add to list for all active servers
    		activeECSNodeList.add(newNode);
    		
    		//add to meta data
    		metaData.put(hash,"");
    		
    		//add to list for newly added servers
    		addedList.add(newNode);
    		   		
    		if (ECSNodeList.size() == 0){
    			System.out.printf("Only able to add %d servers%n",i);
    			break;
    		}	
    	}
    	
    	//update hash ring
    	updateHRange();
    	 
        return addedList;
    }

    @Override
    public boolean awaitNodes(int count, int timeout) throws Exception {
       
    	long startTime = System.currentTimeMillis();
    	long elapsedTime = 0l;
    	
    	while ((int)elapsedTime < timeout)
    	{
    		
    		int rdyServCount=0;
    		
    		for (IECSNode servNode:activeECSNodeList)
    		{
    			
    			String statusPath = "/servers/" + servNode.getNodeName() + "/status";
    			
    			byte[] statusData = zk.getData(statusPath, false, null);
    			
    			String status = statusData.toString();
    		
    			if (status.equals("added"))
    			{
    				System.out.println(servNode.getNodePort());
    				rdyServCount++;
    			}
    		}
    		
    		if (rdyServCount == activeECSNodeList.size())
    		{
    			return true;
    		}
    		
    		elapsedTime = (new Date()).getTime() - startTime;
    	}
    	
        return false;
    }

    @Override
    public boolean removeNodes(Collection<String> nodeNames) throws Exception {
        	
    	// find the node to be removed
    	// remove from active ECSNode List
    	// add back to all available ECSNode List
    	// set the status to none
    	// update zk tree
    	
    	for (int i = nodeNames.size()-1; i>=0; i--)
    	{
    		
    		String deleteNodeName = ((ArrayList<String>)nodeNames).get(i);
    		String nodeName;
    		IECSNode servNode;
    		
    		for (int j=0; i<activeECSNodeList.size(); j++)
    		{
    			
    			servNode = ((ArrayList<IECSNode>)activeECSNodeList).get(j);
    			nodeName = servNode.getNodeName();
    			
    			if (deleteNodeName.equals(nodeName))
    			{
    				
    				//add serverNode back to list of all available nodes
    				ECSNodeList.add(servNode);
    				
    				//delete from list of active nodes
    				((ArrayList<IECSNode>)activeECSNodeList).remove(j);
    				//delete from metaData
    				metaData.remove(uti.cHash(nodeName));
    				
    				
    				String statusPath = "/servers/" + nodeName + "/status";
    				String strategyPath = "/servers/" + nodeName + "/strategy";
    				String sizePath = "/servers/" + nodeName + "/size";
    				String namePath = "/servers/" + nodeName;
    				
    				String status = "removing";
    				
    				byte[] statusData = status.getBytes();
			
					zk.setData(statusPath, statusData, zk.exists(statusPath, false).getVersion());
					
					while (true)
					{
						statusData = zk.getData(statusPath, false, null);
						
						status = statusData.toString();
						
						if (status.equals("removed"))
						{
							
							zk.setData(statusPath, null, (zk.exists(statusPath, false).getVersion()));
							zk.delete(statusPath, (zk.exists(statusPath, false).getVersion()));
							
							zk.setData(sizePath, null, (zk.exists(sizePath, false).getVersion()));
							zk.delete(sizePath, (zk.exists(sizePath, false).getVersion()));
							
							zk.setData(strategyPath, null, (zk.exists(strategyPath, false).getVersion()));
							zk.delete(strategyPath, (zk.exists(strategyPath, false).getVersion()));
							
							zk.setData(namePath, null, (zk.exists(namePath, false).getVersion()));
							zk.delete(namePath, (zk.exists(namePath, false).getVersion()));
							
							System.out.println(nodeName + " removed");
							break;
						}
					}
									
					((ArrayList<String>)nodeNames).remove(i);
					break;
    			}
    		}
    	}
    	
    	if (nodeNames.size() == 0){
    		System.out.println("Nodes removed successfully");
    		return true;
    	}
    	
    	System.out.println("The following server names cannot be found and removed");
    	System.out.println(nodeNames);
    	
        return false;
    }

    @Override
    public Map<String, IECSNode> getNodes() {
        
    	//traverse activeECSNode List
    	//create map entry for each node
    	
    	Map <String, IECSNode> nodeMap = new HashMap<String,IECSNode>();
    	
    	for (IECSNode servNode: activeECSNodeList){
    		
    		nodeMap.put(servNode.getNodeName(), servNode);
    		
    	}
        return nodeMap;
    }

    @Override
    public IECSNode getNodeByKey(String Key) throws UnsupportedEncodingException, NoSuchAlgorithmException {
        
    	//convert key into hash
    	//traverse activeECSNode List
    	//continue if keyHash is greated than serverHash
 
		String keyHash = uti.cHash(Key);
			
	    for (IECSNode servNode: activeECSNodeList)
	    {
	    	if (keyHash.compareTo(servNode.getNodeHashValue())<=0)
	    		return servNode;
	    }
	    	
    	
    	return ((ArrayList<IECSNode>)activeECSNodeList).get(0);   	
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
    					Collection<IECSNode> newNodesList;
    					
    					int numNodes = Integer.parseInt(sElements[1]);
    					String cacheStrgy = sElements[2];
    					int cacheSze = Integer.parseInt(sElements[3]);
    					
    					if (cacheStrgy.equals("FIFO")||cacheStrgy.equals("LRU")||cacheStrgy.equals("LFU"))
    					{
    				
    						//write set ECSNodes function
    						newNodesList = addNodes(numNodes,cacheStrgy,cacheSze);
    					
    						if (newNodesList != null)
    						{
    							System.out.println("nodes added successfully");
    							System.out.println("Following nodes are active: ");
    						
    							for (IECSNode servNode:activeECSNodeList)
    							{
    								servNode.printNodeInfo();
    							}

    						}
    						else
    							System.out.println("Add nodes failed");
    					}
    					else
    						System.out.println("invalid strategy entered; Please enter either FIFO, LRU or LFU");
    					
    				}
    				else if(sAction.equals(ADDNODE))
    				{
    					System.out.println("node added successfully");
    					IECSNode newNode;
    					String cacheStrgy = sElements[1];
    					int cacheSze = Integer.parseInt(sElements[2]);
    					
    					newNode = addNode(cacheStrgy,cacheSze);
    					
    					if (newNode != null){
    						
    						System.out.println("node added successfully");
    						System.out.println("Following nodes are active: ");
    						
    						for (IECSNode servNode:activeECSNodeList){
    							servNode.printNodeInfo();
    						}

    					}
    					else 
    						System.out.println("add single node failed");
    					
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
    						System.out.println("nodes successfully removed");
    					else
    						System.out.println("cannot remove nodes");
    						
    				}
    				else if (sAction.equals(GETNODES))
    				{
    					Map <String, IECSNode> nodeMap = getNodes();
    					
    					System.out.println("'get nodes successful");
    					
    					System.out.println(nodeMap);
    					
    				}
    				else if (sAction.equals(GETNODE))
    				{
    					String sKey = sElements[1];
    					
    					IECSNode servNode = getNodeByKey(sKey);
    					
    					System.out.println("get node by key successful");
    					
    					servNode.printNodeInfo();
    					
    				}
    				else if(sAction.equals(SHUTDOWN))
    				{
    					if (shutdown())
    					{
    						System.out.println("Shutdown successful");
    						break;
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
    
    public void updateHRange() throws Exception{
    	
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
    		
    		IECSNode servNode = ((ArrayList<IECSNode>)activeECSNodeList).get(i);
    		servNode.setNodeHashRange(lowerB, upperB);
    		metaData.put(upperB,servNode.getNodeHost()+" "+servNode.getNodePort()+" "+lowerB+" "+upperB);
    	}
    	
    	updateMetaData();
    	
    }
    
    public void updateMetaData() throws Exception{
    	//set metaData znode
    	
    	String metaDataPath = "/servers/metadata";
    	
    	byte[] updated_metaData = uti.SerializeHashMapToByteArray(metaData);
    	
    	zk.setData(metaDataPath, updated_metaData, zk.exists(metaDataPath, true).getVersion());
    	
    	//create zookeeper znodes for server properties
    	for (IECSNode servNode:activeECSNodeList){
    		
    		System.out.println("UMD0");
    	
    		String statusPath = "/servers/" + servNode.getNodeName() + "/status";
    		
    		String sizePath = "/servers/" + servNode.getNodeName()+ "/size";
    		
    		String strategyPath = "/servers/strategy" + servNode.getNodeName() + "/strategy";

     		String status = "adding";
     		
     		String size = Integer.toString(servNode.getNodeCacheSize());
     		
     		String strategy = servNode.getNodeStrategy();
    		  		
    		byte[] statusData = status.getBytes();
    		byte[] sizeData = size.getBytes();
    		byte[] strategyData = strategy.getBytes();

    		try {
				
				if (zk.exists(statusPath, true) != null)
	    			zk.setData(statusPath, statusData, zk.exists(statusPath, true).getVersion());
				else
					zk.create(statusPath, statusData, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
				
				if (zk.exists(sizePath, true) != null)
	    			zk.setData(sizePath, sizeData, zk.exists(sizePath, true).getVersion());
				else
					zk.create(sizePath, sizeData, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
				
				if (zk.exists(strategyPath, true) != null)
	    			zk.setData(strategyPath, strategyData, zk.exists(strategyPath, true).getVersion());
				else
					zk.create(strategyPath, strategyData, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
	    		
			} catch (Exception e) {
				
				e.printStackTrace();
				System.out.println("ECS:failed to update zNodes");
			}	
    	}
    	
    	
    }
    
    public String createScript(IECSNode newNode) throws IOException{
    	
    	try {
    		
    		File configFile = new File("script.sh");
    		
    		if (configFile.exists()){
    			
    		}
    		
    		configFile.createNewFile();
    		
			PrintWriter out = new PrintWriter(new FileOutputStream("script.sh",false));
			
			//out.println("ssh-keygen");
			
			InetAddress ia = InetAddress.getLocalHost();
			String hostname = ia.getHostName();
			
			String sPath = configFile.getAbsolutePath();
			String instruction = String.format("ssh -n %s nohup java -jar %s/m2-server.jar %s %s %s &",newNode.getNodeHost(),sPath.substring(0, sPath.length() - 10), newNode.getNodeName(),hostname,8094);
			out.println(instruction);
			
			out.close();
			return configFile.getAbsolutePath();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.out.println("Cannot create ssh script");
		}
    	
    	return null;
    	
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
