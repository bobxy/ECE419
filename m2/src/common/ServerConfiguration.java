package common;
import java.io.*;
import java.util.*;

import Utilities.Utilities;
import ecs.*;
public class ServerConfiguration implements Serializable{
	private static final long serialVersionUID = 2L;
	private String sAddress;
	private int nPort;
	private String sUpper;
	private String sLower;
	private String sHashValue;
	private Utilities.servStatus sStatus;
	private String sName;
	private Utilities.servStrategy sStrategy;
	private int sCacheSize;
	
	public ServerConfiguration()
	{
		sAddress = "";
		nPort = -1;
		sUpper = "";
		sLower = "";
		sHashValue = "";
		sStatus = Utilities.servStatus.none;
		sName = "";
		sStrategy=Utilities.servStrategy.none;
		sCacheSize=-1;
	}
	
	public ServerConfiguration(String address, int port, String upper, String lower, String HashValue, Utilities.servStatus status, String Name,Utilities.servStrategy strategy,int cachesize)
	{
		sAddress = address;
		nPort = port;
		sUpper = upper;
		sLower = lower;
		sHashValue = HashValue;
		sStatus = status;
		sName = Name;
		sStrategy=strategy;
		sCacheSize=cachesize;
	}
	
	public ServerConfiguration(IECSNode node)
	{
		sAddress = node.getNodeHost();
		nPort = node.getNodePort();
		String[] Bounds = node.getNodeHashRange();
		sUpper = Bounds[1];
		sLower = Bounds[0];
		sHashValue = node.getNodeHashValue();
		//sStatus = node.getNodeStatus();
		sName = node.getNodeName();
		//sStrategy=node.getNodeStrategy();
		sCacheSize=node.getNodeCacheSize();
	}
	public void SetCacheSize(int size)
	{
		sCacheSize=size;
	}
	public int GetCacheSize()
	{
		return sCacheSize;
	}
	public void SetStrategy(String strategy)
	{
		if (strategy.equals("FIFO")){
    		sStrategy = Utilities.servStrategy.FIFO;
    	}
    	else if (strategy.equals("LRU")){
    		sStrategy = Utilities.servStrategy.LRU;
    	}
    	else if (strategy.equals("LFU")){
    		sStrategy = Utilities.servStrategy.LFU;
    	}
    	else if (strategy.equals("none")){
    		sStrategy = Utilities.servStrategy.none;
    	} 
    	
	}
	public Utilities.servStrategy GetStrategy()
	{
		return sStrategy;
	}
	public void SetAddress(String address)
	{
		sAddress = address;
	}
	
	public void SetPort(int port)
	{
		nPort = port;
	}
	
	public void SetUpper(String upper)
	{
		sUpper = upper;
	}
	
	public void SetLower(String lower)
	{
		sLower = lower;
	}
	
	public void SetHashValue(String HashValue)
	{
		sHashValue = HashValue;
	}
	
	public void SetStatus(Utilities.servStatus status)
	{
		/*if (status.equals("added")){
    		sStatus = Utilities.servStatus.added;
    	}
    	else if (status.equals("adding")){
    		sStatus = Utilities.servStatus.adding;
    	}
    	else if (status.equals("removed")){
    		sStatus = Utilities.servStatus.removed;
    	}
    	else if (status.equals("removing")){
    		sStatus = Utilities.servStatus.removing;
    	}
    	else if (status.equals("started")){
    		sStatus = Utilities.servStatus.started;
    	}
    	else if (status.equals("starting")){
    		sStatus = Utilities.servStatus.starting;
    	}
    	else if (status.equals("stopped")){
    		sStatus = Utilities.servStatus.stopped;
    	}
    	else if (status.equals("stopping")){
    		sStatus = Utilities.servStatus.stopping;
    	}
    	else if (status.equals("receiving")){
    		sStatus = Utilities.servStatus.receiving;
    	}
    	else if (status.equals("sending")){
    		sStatus = Utilities.servStatus.sending;
    	}
    	else if (status.equals("none")){
    		sStatus = Utilities.servStatus.none;
    	} */
		sStatus=status;
	}
	
	public void SetName(String Name)
	{
		sName = Name;
	}
	
	public String GetAddress()
	{
		return sAddress;
	}
	
	public int GetPort()
	{
		return nPort;
	}
	
	public String GetUpper()
	{
		return sUpper;
	}
	
	public String GetLower()
	{
		return sLower;
	}
	
	public String GetHashValue()
	{
		return sHashValue;
	}
	
	public Utilities.servStatus GetStatus()
	{
		return sStatus;
	}
	
	public String GetName()
	{
		return sName;
	}
	
	public Boolean IsResponsible(String sHashValue)
	{
		if(sUpper.compareTo(sLower) > 0)
			return (sLower.compareTo(sHashValue) < 0) && (sUpper.compareTo(sHashValue) >= 0);
		else
			return (sLower.compareTo(sHashValue) < 0) || (sUpper.compareTo(sHashValue) >= 0);
	}
}
