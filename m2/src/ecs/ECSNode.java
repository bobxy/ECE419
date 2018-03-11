package ecs;

import java.util.Comparator;

import Utilities.Utilities;
import common.ServerConfiguration;

public class ECSNode implements IECSNode{
	
	private String servName;
	private String servAddr;
	private int servPort;
	private String[] servHashR;
	private String servHashV;
	private int servCacheSize;
	Utilities.servStatus servStatus;
	Utilities.servStrategy servStrategy;
    

	public ECSNode(String name, String address, int port) {
		// TODO Auto-generated constructor stub
		servName = name;
		servAddr = address;
		servPort = port;
		servHashR = new String[2];
		servHashV ="";
		servCacheSize = -1;
		servStatus = Utilities.servStatus.adding;
		servStrategy = Utilities.servStrategy.none;
	}
	
	public ECSNode(ServerConfiguration config)
	{
		servName = config.GetName();
		servAddr = config.GetAddress();
		servPort = config.GetPort();
		servHashR = new String[2];
		servHashR[0] = config.GetLower();
		servHashR[1] = config.GetUpper();
		servStatus = config.GetStatus();
		servCacheSize = config.GetCacheSize();
		servStrategy = config.GetStrategy();
	}
	   /**
     * @return  the name of the node (ie "Server 8.8.8.8")
     */
    public String getNodeName(){
    	return servName;
    }

    /**
     * @return  the hostname of the node (ie "8.8.8.8")
     */
    public String getNodeHost(){
    	return servAddr;
    }

    /**
     * @return  the port number of the node (ie 8080)
     */
    public int getNodePort(){
    	return servPort;
    }

    /**
     * @return  array of two strings representing the low and high range of the hashes that the given node is responsible for
     */
    public String[] getNodeHashRange(){
    	return servHashR;
    }
    
   
    public void setNodeHashRange(String lowerB, String upperB){
    	servHashR[0]=lowerB;
    	servHashR[1]=upperB;
    }
    
    public String getNodeHashValue(){
    	return servHashV;
    }
    
    public void setNodeHashValue(String value){
    	servHashV = value;
    }
    
    public Utilities.servStatus getNodeStatus(){
    	return servStatus;
    }
    
    public void setNodeStatus(String status){
    	
    	if (status.equals("added")){
    		servStatus = Utilities.servStatus.added;
    	}
    	else if (status.equals("adding")){
    		servStatus = Utilities.servStatus.adding;
    	}
    	else if (status.equals("removed")){
    		servStatus = Utilities.servStatus.removed;
    	}
    	else if (status.equals("removing")){
    		servStatus = Utilities.servStatus.removing;
    	}
    	else if (status.equals("started")){
    		servStatus = Utilities.servStatus.started;
    	}
    	else if (status.equals("starting")){
    		servStatus = Utilities.servStatus.starting;
    	}
    	else if (status.equals("stopped")){
    		servStatus = Utilities.servStatus.stopped;
    	}
    	else if (status.equals("stopping")){
    		servStatus = Utilities.servStatus.stopping;
    	}
    	else if (status.equals("receiving")){
    		servStatus = Utilities.servStatus.receiving;
    	}
    	else if (status.equals("sending")){
    		servStatus = Utilities.servStatus.sending;
    	}
    	else if (status.equals("none")){
    		servStatus = Utilities.servStatus.none;
    	} 
    	else if (status.equals("adding_starting")){
    		servStatus = Utilities.servStatus.adding_starting;
    	}
    }
    
    public int getNodeCacheSize(){
    	return servCacheSize;
    }
    
    public void setNodeCacheSize(int cacheSize){
    	servCacheSize = cacheSize;
    }
    
    public Utilities.servStrategy getNodeStrategy(){
    	return servStrategy;
    }
    
    public void setNodeStrategy(String cacheStrategy){
    	
    	if (cacheStrategy.equals("FIFO")){
    		servStrategy = Utilities.servStrategy.FIFO;
    	}
    	else if (cacheStrategy.equals("LRU")){
    		servStrategy = Utilities.servStrategy.LRU;
    	}
    	else if (cacheStrategy.equals("LFU")){
    		servStrategy = Utilities.servStrategy.LFU;
    	}
    	else if (cacheStrategy.equals("none")){
    		servStrategy = Utilities.servStrategy.none;
    	} 
    	
    }

    public void printNodeInfo(){
    	System.out.println(servName + " " + Integer.toString(servPort) + " " + servAddr + " lowerb:" + servHashR[0] + " upperb:" + servHashR[1] + " hash:" + servHashV + " " + servStatus);
    }
}
