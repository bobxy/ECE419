package common;
import java.io.*;
import java.util.*;
import ecs.*;
public class ServerConfiguration implements Serializable{
	private static final long serialVersionUID = 2L;
	private String sAddress;
	private int nPort;
	private String sUpper;
	private String sLower;
	private String sHashValue;
	private String sStatus;
	private String sName;
	private String sStrategy;
	private int sCacheSize;
	public ServerConfiguration()
	{
		sAddress = "";
		nPort = -1;
		sUpper = "";
		sLower = "";
		sHashValue = "";
		sStatus = "";
		sName = "";
		sStrategy="";
		sCacheSize=-1;
	}
	
	public ServerConfiguration(String address, int port, String upper, String lower, String HashValue, String status, String Name,String strategy,int cachesize)
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
		sStatus = node.getNodeStatus();
		sName = node.getNodeName();
		//sStrategy=node.getNodeStrategy;
		//sCacheSize=node.getNodeCacheSize;
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
		sStrategy=strategy;
	}
	public String GetStrategy()
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
	
	public void SetStatus(String status)
	{
		sStatus = status;
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
	
	public String GetStatus()
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
