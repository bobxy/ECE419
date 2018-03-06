package app_kvECS;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
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

	public ECSClient(){
		//establish zookeeper connection and return zookeeper object
		zkC = new ZKConnection();
		try{
			zk = zkC.connect("localhost:2181");
		}catch (Exception e)
		{
			System.out.println("cannot connect to zookeeper!");
		}
		
		//create zookeeper nodes for server and fct
		try {
			zk.create("/servers", null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
			zk.create("/actions", null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		} catch (KeeperException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
		
		ECSNodeList = new ArrayList<IECSNode>();
		activeECSNodeList = new ArrayList<IECSNode>();
	}

    @Override
    public boolean start() {
        // TODO
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
    public IECSNode addNode(String cacheStrategy, int cacheSize) {
    	// 
        return null;
    }

    @Override
    public Collection<IECSNode> addNodes(int count, String cacheStrategy, int cacheSize) {
        //calls setupNodes
    	setupNodes(count,cacheStrategy,cacheSize);
    	
    	//create znode and metadata
    	
    	IECSNode servNode;
    	
    	for (int i=0;i<activeECSNodeList.size(); i++){
    		
    		servNode = ((ArrayList<IECSNode>) activeECSNodeList).get(i);
        	
    		String servName = servNode.getNodeName();
    		String servAddr = servNode.getNodeHost();
    		String servPort = Integer.toString(servNode.getNodePort());
    		String[] servHashR = servNode.getNodeHashRange();
		
    		byte[] data = (servName + " " + servAddr + " " + servPort + " " + servHashR[0] + " " +servHashR[1]).getBytes();
		
    	//create zookeeper znode
    		try {
    			zk.create("/servers/server_", data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL);
    		} catch (KeeperException e) {
    			e.printStackTrace();
    		} catch (InterruptedException e) {
    			e.printStackTrace();
    		}
    	}
    	//do ssh
    	
    	//calls awaitNodes
        return null;
    }

    @Override
    public Collection<IECSNode> setupNodes(int count, String cacheStrategy, int cacheSize) {
    	
    	if (count == 0){
    		System.out.println("No server added");
    		return activeECSNodeList;
    	}
    	
    	if (ECSNodeList.size() == 0){
    		System.out.println("No more available server; add failed");
    		return activeECSNodeList;
    	}
    	
    	IECSNode newNode;
        
    	for (int i=1; i<= count; i++)
    	{
    		int idx = ThreadLocalRandom.current().nextInt(0, ECSNodeList.size());
    		
    		newNode =  ((ArrayList<IECSNode>) ECSNodeList).get(idx); 
    		((ArrayList<IECSNode>) ECSNodeList).remove(idx);
    		
    		activeECSNodeList.add(newNode);
    			
    		if (ECSNodeList.size() == 0){
    			System.out.printf("Only able to add %d servers%n",i);
    			break;
    		}
    		
    	} 
    	
    	//perform consistent hashing
    	
        return activeECSNodeList;
    }

    @Override
    public boolean awaitNodes(int count, int timeout) throws Exception {
        // TODO
        return false;
    }

    @Override
    public boolean removeNodes(Collection<String> nodeNames) {
        // TODO
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
    	System.out.println(ADDNODES + " number of nodes" + " size of cache" + " replacement strategy");
    	System.out.println(ADDNODE + " size of cache" + " replacement strategy");
    	System.out.println(REMOVENODE + " node name 1" + " node name 2" + " ...");
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
    	
    	while ((currentLine = br.readLine()) != null)
    	{
    		
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
    
    /////////////main

    public static void main(String[] args) {
        // TODO
    	
    	ECSClient cli = new ECSClient();
    	
    	try {
			cli.readConfig(args[0]);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	
    	cli.run();
    }
}
