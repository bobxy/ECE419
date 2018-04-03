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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Collection;
import java.util.Collections;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.*;

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
	//private Collection<IECSNode> activeECSNodeList;
	private HashMap<String, String> metaData;
	private HashMap<String, String> timeStamps;
	private Utilities uti;
	private FailureDetect FD;
	//private boolean init;

	private HashMap<String,String>HashToNodeName;
	public ECSClient() throws IOException, InterruptedException,
			KeeperException {
		// establish zookeeper connection and return zookeeper object
		zkC = new ZKConnection();
		zk = zkC.connect("127.0.0.1:8097");

		// create zookeeper nodes for server and meta data
		zk.create("/servers", null, ZooDefs.Ids.OPEN_ACL_UNSAFE,
				CreateMode.PERSISTENT);
		zk.create("/servers/metadata", null, ZooDefs.Ids.OPEN_ACL_UNSAFE,
				CreateMode.PERSISTENT);

		uti = new Utilities();
		ECSNodeList = new ArrayList<IECSNode>();
		//activeECSNodeList = new ArrayList<IECSNode>();
		metaData = new HashMap<String, String>();
		HashToNodeName= new HashMap<String,String>();
		timeStamps = new HashMap<String,String>();
		FD = new FailureDetect(this);
		FD.start();
		//init = true;
	}

	@Override
	public boolean start_ecs() throws Exception {

		List<String> childnodenames = zk.getChildren("/servers", false);
		for (String nodename : childnodenames) {
			System.out.println("current nodename in starting is: " + nodename);
			if (!nodename.equals("metadata")) {
				String currentStatus=GetStatus(nodename);
				if(currentStatus.equals("added"))
				{
					SetStatus(nodename,"starting");
				}
			}
		}
		InvokeInterrupt();
		int count=0;
		while(count!=(childnodenames.size()-1))
		{
			count=0;
			for (String nodename : childnodenames) {
				System.out.println("current nodename in starting is: " + nodename);
				if (!nodename.equals("metadata")) {
					// only one node active, just shut it down, no need to move data
					// change status to removing
					if(!GetStatus(nodename).equals("starting"))
					{
						count+=1;
					}
				}
			}
			sleep();
		}
		return true;
	}

	@Override
	public boolean stop_ecs() throws Exception, InterruptedException {

		List<String> childnodenames = zk.getChildren("/servers", false);
		for (String nodename : childnodenames) {
			System.out.println("current nodename in starting is: " + nodename);
			if (!nodename.equals("metadata")) {
				SetStatus(nodename,"stopping");
			}
		}
		InvokeInterrupt();
		int count=0;
		while(count!=(childnodenames.size()-1))
		{
			count=0;
			for (String nodename : childnodenames) {
				System.out.println("current nodename in starting is: " + nodename);
				if (!nodename.equals("metadata")) {
					// only one node active, just shut it down, no need to move data
					// change status to removing
					if(!GetStatus(nodename).equals("stopping"))
					{
						count+=1;
					}
				}
			}
			sleep();
		}
		return true;
	}

	@Override
	public boolean shutdown() throws Exception {
		List<String> children=zk.getChildren("/servers", false);
		
		for(String child:children)
		{
			if(!child.equals("metadata"))
			{

				SetStatus(child,"exiting");
				InvokeInterrupt();
				while(GetStatus(child).equals("exiting"))
				{
					sleep();
				}
				DeletePath(child);
			}
		}
		
		zk.setData("/servers/metadata", null, -1);
		zk.delete("/servers/metadata", -1);
		
		//delete /servers
		zk.setData("/servers", null, -1);
		zk.delete("/servers", -1);
		
		zk.close();
		zk = null;
		
		return true;
	}

	@Override
	public IECSNode addNode(String cacheStrategy, int cacheSize)
			throws Exception {
		int idx = ThreadLocalRandom.current().nextInt(0, ECSNodeList.size());
		
		System.out.print("ecsnodelist size is: "+ECSNodeList.size());
		System.out.print("current index is: "+idx);
		
		IECSNode newNode = ((ArrayList<IECSNode>) ECSNodeList).get(idx);
		System.out.println("current node name is: "+newNode.getNodeName());
		
		((ArrayList<IECSNode>) ECSNodeList).remove(idx);
		CreatePath(newNode.getNodeName(), "adding", cacheSize, cacheStrategy);
		metaData.put(uti.cHash(newNode.getNodeName()),newNode.getNodeHost()+" "+newNode.getNodePort());
		updateHRange();
		String ServerInfo=metaData.get(uti.cHash(newNode.getNodeName()));
		String[] ServerInfos=ServerInfo.trim().split("\\s+");
		for(String info:ServerInfos)
		{
			System.out.println("info is: "+info);
		}
		newNode.setNodeCacheSize(cacheSize);
		newNode.setNodeHashRange(ServerInfos[2], ServerInfos[3]);
		newNode.setNodeHashValue(ServerInfos[3]);
		newNode.setNodeStrategy(cacheStrategy);
		newNode.setNodeStatus("adding");
		Runtime run=Runtime.getRuntime();
		Process proc= run.exec(createScript(newNode));
		proc.waitFor();
		InvokeInterrupt();
		while(GetStatus(newNode.getNodeName()).equals("adding"))
		{
			sleep();
		}
		timeStamps.put(uti.cHash(newNode.getNodeName()), "0");
		return newNode;
	}

	public IECSNode add1Node(String cacheStrategy,int cacheSize) throws Exception
	{
		IECSNode temp=addNode(cacheStrategy,cacheSize);
		
		//need to fix
		if(metaData.size()!=1)
		{
			String SenderHash=uti.FindNextOne(metaData, temp.getNodeHashValue());
			String Sender=HashToNodeName.get(SenderHash);
			SetStatus(Sender,"sending");

			InvokeInterrupt();
			System.out.println("before sending");
			while(GetStatus(Sender).equals("sending"))
			{
				sleep();
			}

		}

		SetStatus(temp.getNodeName(),"starting");
		InvokeInterrupt();

		while(GetStatus(temp.getNodeName()).equals("starting"))
		{
			sleep();
		}

		return temp;
	}
	public void updateHRange() throws Exception {

		if(metaData.size()==0)
			return;
		//System.out.println(activeECSNodeList);
		ArrayList<String> myservers=new ArrayList<String>();
		Set<String> setServers=metaData.keySet();
		Iterator<String> ServerIterator=setServers.iterator();
		while(ServerIterator.hasNext())
		{
			myservers.add(ServerIterator.next());
		}
		
		
		// sort the IECSNodeList according to hashvalue of each server in
		// ascending
		Collections.sort(myservers);


		// find the hash range for each server (construct the ring)
		if(metaData.size()==1)
		{
			//only 1 element
			String Upper=myservers.get(0);
			String value=metaData.get(myservers.get(0));
			String[] ServerInfos=value.trim().split("\\s+");
			value=ServerInfos[0]+" "+ServerInfos[1]+" "+Upper+" "+Upper;
			metaData.put(Upper,
					value);
			return;
		}
		
		for(int i=1;i<myservers.size();i++)
		{
			String Upper=myservers.get(i);
			String Lower=myservers.get(i-1);
			String value=metaData.get(Upper);
			String[] ServerInfos=value.trim().split("\\s+");

			value=ServerInfos[0]+" "+ServerInfos[1]+" "+Lower+" "+Upper;
			metaData.put(Upper,
					value);
		}
		
		String Upper=myservers.get(0);
		String Lower=myservers.get(myservers.size()-1);
		String value=metaData.get(Upper);
		
		String[] ServerInfos=value.trim().split("\\s+");

		value=ServerInfos[0]+" "+ServerInfos[1]+" "+Lower+" "+Upper;
		metaData.put(Upper,
				value);
	}

	@Override
	public Collection<IECSNode> addNodes(int count, String cacheStrategy,
			int cacheSize) throws Exception {

		Collection<IECSNode> newNodes = new ArrayList<IECSNode>();

		for (int i = 0; i < count; i++) {
			newNodes.add(addNode(cacheStrategy, cacheSize));
		}
		return newNodes;
	}

	@Override
	public Collection<IECSNode> setupNodes(int count, String cacheStrategy,
			int cacheSize) throws Exception {
		return null;
	}

	@Override
	public boolean awaitNodes(int count, int timeout) throws Exception {

		return true;
	}

	@Override
	public boolean removeNodes(Collection<String> nodeNames) throws Exception {

			// remove from activate ecsnode list
			// remove from metadata
			// add back to all available ecsnode list
			// update zk tree

			// check if its in active node list
			for(String servername:nodeNames)
			{
				if(metaData.containsKey(uti.cHash(servername)))
				{

					SetStatus(servername,"removing");

					InvokeInterrupt();

					while(GetStatus(servername).equals("removing"))
					{
						sleep();
					}

					String ServerInfo=metaData.get(uti.cHash(servername));

					String[] ServerInfos=ServerInfo.trim().split("\\s+");
					ECSNode servNode = new ECSNode(servername, ServerInfos[0], Integer.parseInt(ServerInfos[1]));
	
					ECSNodeList.add(servNode);
					
					metaData.remove(uti.cHash(servername));
					DeletePath(servername);
					updateHRange();
					InvokeInterrupt();
				}
			}
		return true;
	}

	@Override
	public Map<String, IECSNode> getNodes() throws KeeperException,
			InterruptedException {


		Map<String, IECSNode> nodeMap = new HashMap<String, IECSNode>();
		Set<String> children = HashToNodeName.keySet();
		for (String child : children) {
			if(metaData.containsKey(child))
			{
				String ServerInfo=metaData.get(child);
				String[] ServerInfos=ServerInfo.trim().split("\\s+");
				
				ECSNode servNode = new ECSNode(HashToNodeName.get(child), ServerInfos[0], Integer.parseInt(ServerInfos[1]));
				servNode.setNodeHashRange(ServerInfos[2],ServerInfos[3]);
				
				nodeMap.put(HashToNodeName.get(child), servNode);
			}
		}
		return nodeMap;
	}

	@Override
	public IECSNode getNodeByKey(String Key)
			throws UnsupportedEncodingException, NoSuchAlgorithmException {

		// convert key into hash
		// traverse activeECSNode List
		// continue if keyHash is greated than serverHash

		String keyHash = uti.cHash(Key);
		Set<String> keySet=metaData.keySet();
		for(String key:keySet)
		{
			String ServerInfo=metaData.get(key);
			String[] ServerInfos=ServerInfo.trim().split("\\s+");
			String currentLower=ServerInfos[2];
			String currentUpper=ServerInfos[3];
			if(metaData.size() == 1 || uti.IsResponsible(keyHash, currentLower, currentUpper))
			{
				ECSNode servNode = new ECSNode(HashToNodeName.get(key), ServerInfos[0], Integer.parseInt(ServerInfos[1]));
				return servNode;
			}
		}
		return null;
	}

	public void run() {
		try {
			while (true) {
				System.out.print(PROMPT);
				BufferedReader stdin = new BufferedReader(
						new InputStreamReader(System.in));
				String cmdLine = stdin.readLine();
				String[] sElements = Parse(cmdLine);
				if (sElements != null) {
					String sAction = sElements[0];
					if (sAction.equals(HELP)) {
						help();
						Set<String> temp = metaData.keySet();
						for (String key : temp) {
							System.out.println("hash key is: " + key);
							String ServerInfo = metaData.get(key);
							String[] ServerInfos = ServerInfo.trim().split(
									"\\s+");

							System.out.println("IP is: " + ServerInfos[0]);
							System.out.println("PORT is: " + ServerInfos[1]);
							System.out.println("Lower Bound is: " + ServerInfos[2]);
							System.out.println("Upper Bound is: " + ServerInfos[3]);
						}
					}

					else if (sAction.equals(START)) {
						if (start_ecs()) {
							System.out.println("All servers started");
						} else
							System.out.println("Can not start servers");
					} else if (sAction.equals(STOP)) {
						if (stop_ecs()) {
							System.out.println("All servers stopped");
						} else
							System.out.println("Can not stop servers");
					} else if (sAction.equals(ADDNODES)) {
						
						Collection<IECSNode> newNodesList;

						int numNodes = Integer.parseInt(sElements[1]);
						
						if(numNodes>ECSNodeList.size())
						{
							System.out
							.println("not enough configurations");
						}
						else
						{
							String cacheStrgy = sElements[2];
							int cacheSze = Integer.parseInt(sElements[3]);

							if (cacheStrgy.equals("FIFO")
									|| cacheStrgy.equals("LRU")
									|| cacheStrgy.equals("LFU")) {

								// write set ECSNodes function
								newNodesList = addNodes(numNodes, cacheStrgy,
										cacheSze);
								setReplicas();
								moveReplicas();
							} else
								System.out
										.println("invalid strategy entered; Please enter either FIFO, LRU or LFU");
						}


					} else if (sAction.equals(ADDNODE)) {
						IECSNode newNode;
						String cacheStrgy = sElements[1];
						int cacheSze = Integer.parseInt(sElements[2]);

						newNode = add1Node(cacheStrgy, cacheSze);
						// start();
						setReplicas();
						moveReplicas();
						if (newNode != null) {

							System.out.println("node added successfully");
							
						} else
							System.out.println("add single node failed");

					} else if (sAction.equals(REMOVENODE)) {
						Collection<String> nodeNames = new ArrayList<String>();

						for (int i = 1; i < sElements.length; i++) {
							nodeNames.add(sElements[i]);
						}

						boolean removed = removeNodes(nodeNames);
						setReplicas();
						moveReplicas();
						if (removed)
							System.out.println("nodes successfully removed");
						else
							System.out.println("cannot remove nodes");

					} else if (sAction.equals(GETNODES)) {
						Map<String, IECSNode> nodeMap = getNodes();
						Set<String> keySet=nodeMap.keySet();
						for(String key:keySet)
						{
							IECSNode currentNode=nodeMap.get(key);
							currentNode.printNodeInfo();
							System.out.println(GetReplicaStatus("/servers/"+currentNode.getNodeName()));
						}
						System.out.println("'get nodes successful");

					} else if (sAction.equals(GETNODE)) {
						String sKey = sElements[1];

						IECSNode servNode = getNodeByKey(sKey);

						System.out.println("get node by key successful");

						servNode.printNodeInfo();

					} else if (sAction.equals(SHUTDOWN)) {
						if (shutdown()) {
							System.out.println("Shutdown successful");
							break;
						} else
							System.out.println("Can not shutdown system");
					}
				} else
					System.out.println("Error. Type help for instructions.");
			}
			FD.stop();
		} catch (Exception e) {
			System.out.println("Error!");
			e.printStackTrace();
			System.exit(1);
		}
	}

	private String[] Parse(String sCommand) {
		try {
			sCommand = sCommand.trim();
			String[] sElements = sCommand.split(" ");
			int nElementCount = sElements.length;

			if (sCommand.equals(START) || sCommand.equals(STOP)
					|| sCommand.equals(SHUTDOWN) || sCommand.equals(GETNODES)
					|| sCommand.equals(HELP)) {
				String[] ret = { sCommand };
				return ret;
			}

			// Error
			if (nElementCount < 2)
				return null;

			String sAction = sElements[0];

			// addNodes command
			if (sAction.equals(ADDNODES) && nElementCount == 4) {
				String numNodes = sElements[1];
				String cacheSze = sElements[2];
				String cacheStrgy = sElements[3];
				String[] ret = { sAction, numNodes, cacheSze, cacheStrgy };
				return ret;
			}

			// add single node command
			if (sAction.equals(ADDNODE) && nElementCount == 3) {
				String cacheSze = sElements[1];
				String cacheStrgy = sElements[2];
				String[] ret = { sAction, cacheSze, cacheStrgy };
				return ret;
			}

			// remove node command
			if (sAction.equals(REMOVENODE)) {
				return sElements;
			}

			// get node command
			if (sAction.equals(GETNODE) && nElementCount == 2) {
				String sKey = sElements[1];
				String[] ret = { sAction, sKey };
				return ret;
			}

		} catch (Exception e) {
			System.out.println("Error when parsing command!");
			e.printStackTrace();
			System.exit(1);
		}
		return null;
	}

	private void help() {
		System.out
				.println("Please follow the following formats for the instructions");
		System.out.println(ADDNODES + " <number of nodes>"
				+ " <replacement strategy>" + " <size of cache>");
		System.out.println(ADDNODE + " <replacement strategy>"
				+ " <size of cache>");
		System.out.println(REMOVENODE + " <node name 1>" + " <node name 2>"
				+ " ...");
		System.out.println(GETNODE + " <key>");
		System.out.println(START);
		System.out.println(STOP);
		System.out.println(SHUTDOWN);
		System.out.println(GETNODES);
	}

	public void readConfig(String file) throws IOException, NoSuchAlgorithmException {

		File servConfig = new File(file);
		FileReader fr = new FileReader(servConfig);
		BufferedReader br = new BufferedReader(fr);
		String currentLine;

		System.out.println("readconfig");
		while ((currentLine = br.readLine()) != null) {
			System.out.println(currentLine);

			StringTokenizer st = new StringTokenizer(currentLine);

			String servName = st.nextToken();
			String servAddress = st.nextToken();
			String port = st.nextToken();

			int servPort = Integer.parseInt(port);

			ECSNode servNode = new ECSNode(servName, servAddress, servPort);
			HashToNodeName.put(uti.cHash(servName), servName);
			ECSNodeList.add(servNode);

		}

		br.close();
		fr.close();
	}


	public String createScript(IECSNode newNode) throws IOException {

		try {

			File configFile = new File("script.sh");

			if (!configFile.exists()) {
				configFile.createNewFile();
			}

			PrintWriter out = new PrintWriter(new FileOutputStream("script.sh",
					false));

			// out.println("ssh-keygen");

			InetAddress ia = InetAddress.getLocalHost();
			String hostname = ia.getHostName();

			String sPath = configFile.getAbsolutePath();
			String instruction = String.format(
					"ssh -n %s nohup java -jar %s/m2-server.jar %s %s %s &",
					newNode.getNodeHost(),
					sPath.substring(0, sPath.length() - 10),
					newNode.getNodeName(), hostname, 8097);
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

	private void SetReplica(String serverName,String ReplicaHash,String order) throws KeeperException, InterruptedException
	{
		//order=replica1
		//order=replica2
		String path="/servers/"+serverName+"/"+order;
		zk.setData(path, ReplicaHash.getBytes(), -1);
	}
	private void SetStatus(String serverName, String status)
			throws KeeperException, InterruptedException {
		String path = "/servers/" + serverName + "/status";
		zk.setData(path, status.getBytes(), -1);
	}

	private String GetStatus(String serverName) throws KeeperException, InterruptedException
	{
		String res=new String(zk.getData("/servers/"+serverName+"/status", false, zk.exists("/servers/"+serverName+"/status", false)));
		return res;
	}
	private void InvokeInterrupt() throws KeeperException, InterruptedException, Exception
	{
		zk.setData("/servers/metadata",uti.SerializeHashMapToByteArray(metaData), -1);
	}
	private void DeletePath(String serverName) throws KeeperException, InterruptedException
	{
		// now remove dependencies,status,strategy,size
		zk.setData("/servers/" + serverName + "/status", null, -1);
		zk.delete("/servers/" + serverName + "/status", -1);

		zk.setData("/servers/" + serverName + "/strategy", null, -1);
		zk.delete("/servers/" + serverName + "/strategy", -1);

		zk.setData("/servers/" + serverName + "/size", null, -1);
		zk.delete("/servers/" + serverName + "/size", -1);
		
		zk.setData("/servers/"+serverName+"/replica1", null, -1);
		zk.delete("/servers/"+serverName+"/replica1", -1);
		
		zk.setData("/servers/"+serverName+"/replica2", null, -1);
		zk.delete("/servers/"+serverName+"/replica2", -1);
		
		zk.setData("/servers/"+serverName+"/crash", null, -1);
		zk.delete("/servers/"+serverName+"/crash", -1);
		
		zk.delete("/servers/" + serverName, -1);
	}
	private void CreatePath(String serverName, String status, int size,
			String strategy) throws KeeperException, InterruptedException {
		String ServerPath = "/servers/" + serverName;
		String statusPath = ServerPath + "/status";
		String sizePath = ServerPath + "/size";
		String strategyPath = ServerPath + "/strategy";
		String replica1Path=ServerPath+"/replica1";
		String replica2Path=ServerPath+"/replica2";
		String crashPath=ServerPath+"/crash";
		zk.create(ServerPath, null, ZooDefs.Ids.OPEN_ACL_UNSAFE,
				CreateMode.PERSISTENT);
		zk.create(statusPath, status.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE,
				CreateMode.PERSISTENT);
		zk.create(sizePath, Integer.toString(size).getBytes(),
				ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		zk.create(strategyPath, strategy.getBytes(),
				ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		zk.create(replica1Path, "".getBytes(),
				ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		zk.create(replica2Path, "".getBytes(),
				ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		zk.create(crashPath, "0".getBytes(),
				ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
	}

	public void sleep()
	{
		for(int i=0;i<10000;i++)
		{
			
		}
	}
	// ///////////main

	public void setReplicas() throws Exception
	{
	
		//hashed servername
		Set<String> keys=metaData.keySet();
		//no server exist
		
		if(keys.size()==0)
		{
			return;
		}
		
		if(keys.size()==1)
		{	
			Iterator<String> keyIterator=keys.iterator();
			String ReplicaName=keyIterator.next();
			SetReplica(HashToNodeName.get(ReplicaName),"","replica1");
			SetReplica(HashToNodeName.get(ReplicaName),"","replica2");
			SetReplicaStatus();
			return;
		}
		
		if(keys.size()==2)
		{
			Iterator<String> keyIterator=keys.iterator();
			String serverNameHash1=keyIterator.next();
			String serverNameHash2=keyIterator.next();
			
			SetReplica(HashToNodeName.get(serverNameHash1),serverNameHash2,"replica1");
			SetReplica(HashToNodeName.get(serverNameHash1),"","replica2");
			
			SetReplica(HashToNodeName.get(serverNameHash2),serverNameHash1,"replica1");
			SetReplica(HashToNodeName.get(serverNameHash2),"","replica2");
			SetReplicaStatus();
			return;
		}
		for(String key:keys)
		{
			String replica1 = uti.FindNextOne(metaData, key);
			SetReplica(HashToNodeName.get(key),replica1,"replica1");
			String replica2 = uti.FindNextOne(metaData, replica1);
			SetReplica(HashToNodeName.get(key),replica2,"replica2");
		}
		SetReplicaStatus();
		return;
	}

	public void SetReplicaStatus() throws Exception
	{
		Set<String>hashkeys=metaData.keySet();
		for(String hashKey:hashkeys)
		{
			SetStatus(HashToNodeName.get(hashKey),"settingReplica");
		}
		InvokeInterrupt();
		int count=0;
		while(count!=metaData.size())
		{
			count=0;
			for(String hashKey:hashkeys)
			{
				if(!GetStatus(HashToNodeName.get(hashKey)).equals("settingReplica"))
				{
					count+=1;
				}
			}
			sleep();
		}
	}
	public String GetReplicaStatus(String ServerPath) throws KeeperException, InterruptedException
	{
		String HashReplica1=new String( zk.getData(ServerPath+"/replica1", false, zk.exists(ServerPath+"/replica1", false)));
		String HashReplica2=new String( zk.getData(ServerPath+"/replica2", false, zk.exists(ServerPath+"/replica2", false)));
		return "Replica1 is: "+HashReplica1+",Replica2 is: "+HashReplica2;
	}
	
	public void moveReplicas() throws Exception
	{
		Set<String> keys=metaData.keySet();
		for(String hashServerName:keys)
		{
			String ServerName=HashToNodeName.get(hashServerName);
			SetStatus(ServerName,"sendingReplica");
			InvokeInterrupt();
			while(GetStatus(ServerName).equals("sendingReplica"))
			{
				sleep();
			}
		}
	}
	public static void main(String[] args) throws IOException,
			InterruptedException, KeeperException, NoSuchAlgorithmException {

		ECSClient cli = new ECSClient();
		try {
			cli.readConfig(args[0]);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.out.println("cannot load config file");
		}

		cli.run();
	}
	
	// not same means true, updated
	boolean checkTimeStamp(String ServerHash) throws KeeperException, InterruptedException
	{
		String currentTimeStamp=new String(zk.getData("/servers/"+HashToNodeName.get(ServerHash)+"/crash", false, zk.exists("/servers/"+HashToNodeName.get(ServerHash)+"/crash", false)));
		boolean res=!timeStamps.get(ServerHash).equals(currentTimeStamp);
		timeStamps.put(ServerHash, currentTimeStamp);
		return res;
	}
	
	public ArrayList<String> CheckCrash() throws KeeperException, InterruptedException
	{
		Set<String> ServerHash = metaData.keySet();
		ArrayList<String> res=new ArrayList<String>();
		for(String server: ServerHash)
		{
			if(!checkTimeStamp(server))
			{
				//crashed detected
				System.out.println("Detected crashed server: "+HashToNodeName.get(server));
				res.add(HashToNodeName.get(server));
			}
		}
		return res;
	}
	
	public void ExecuteCrash() throws NumberFormatException, UnsupportedEncodingException, NoSuchAlgorithmException, Exception
	{
		ArrayList<String> mycrashedlist=CheckCrash();
		if(mycrashedlist.size()==0)
		{
			return;
		}
		ArrayList<String> strategy_sizeList=new ArrayList<String> ();
		for(String servername:mycrashedlist)
		{
			if(metaData.containsKey(uti.cHash(servername)))
			{
				
				String ServerInfo=metaData.get(uti.cHash(servername));
				
				String strategy=new String(zk.getData("/servers/"+servername+"/strategy", false, zk.exists("/servers/"+servername+"/strategy", false)));
				String cache_size=new String(zk.getData("/servers/"+servername+"/size", false, zk.exists("/servers/"+servername+"/size", false)));
				strategy_sizeList.add(strategy+" "+cache_size);
				
				String[] ServerInfos=ServerInfo.trim().split("\\s+");
				ECSNode servNode = new ECSNode(servername, ServerInfos[0], Integer.parseInt(ServerInfos[1]));
				
				ECSNodeList.add(servNode);
				
				metaData.remove(uti.cHash(servername));
				timeStamps.remove(uti.cHash(servername));
				DeletePath(servername);
				
			}
		}
		
		updateHRange();
		InvokeInterrupt();
		for(String strateg_size:strategy_sizeList)
		{
			String[] ServerInfos=strateg_size.trim().split("\\s+");
			add1Node(ServerInfos[0], Integer.parseInt(ServerInfos[1]));
		}
		setReplicas();
		moveReplicas();
		System.out.println("Crash recovered");
		System.out.print(PROMPT);
	}
	
}
