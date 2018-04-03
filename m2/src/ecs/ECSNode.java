package ecs;

import java.util.Comparator;

import Utilities.Utilities;

public class ECSNode implements IECSNode{
	
	private String servName;
	private String servAddr;
	private int servPort;
	private String[] servHashR;
	private String servHashV;
	private int servCacheSize;
	String servStatus;
	String servStrategy;
    

	public ECSNode(String name, String address, int port) {
		// TODO Auto-generated constructor stub
		servName = name;
		servAddr = address;
		servPort = port;
		servHashR = new String[2];
		servHashV ="";
		servCacheSize = -1;
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
    
    public String getNodeStatus(){
    	return servStatus;
    }
    
    public void setNodeStatus(String status){
    	servStatus=status;
    }
    
    public int getNodeCacheSize(){
    	return servCacheSize;
    }
    
    public void setNodeCacheSize(int cacheSize){
    	servCacheSize = cacheSize;
    }
    
    public String getNodeStrategy(){
    	return servStrategy;
    }
    
    public void setNodeStrategy(String cacheStrategy){
    	servStrategy = cacheStrategy;
    }

    public void printNodeInfo(){
		System.out.println("Current node name is: "+getNodeName());
		System.out.println("Current node IP is: "+getNodeHost());
		System.out.println("Current node port is: "+getNodePort());
		System.out.println("Current lower bound is: "+getNodeHashRange()[0]);
		System.out.println("Current upper bound is: "+getNodeHashRange()[1]);
    }
}
