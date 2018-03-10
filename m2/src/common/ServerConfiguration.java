package common;
import java.io.*;
import java.util.*;

public class ServerConfiguration implements Serializable{
	private static final long serialVersionUID = 2L;
	private String sAddress;
	private int nPort;
	private String sUpper;
	private String sLower;
	private String sHashValue;
	private String sStatus;
	
	public ServerConfiguration()
	{
		sAddress = "";
		nPort = -1;
		sUpper = "";
		sLower = "";
		sHashValue = "";
		sStatus = "";
	}
	
	public ServerConfiguration(String address, int port, String upper, String lower, String HashValue, String status)
	{
		sAddress = address;
		nPort = port;
		sUpper = upper;
		sLower = lower;
		sHashValue = HashValue;
		sStatus = status;
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
	
	public Boolean IsResponsible(String sHashValue)
	{
		if(sUpper.compareTo(sLower) > 0)
			return (sLower.compareTo(sHashValue) < 0) && (sUpper.compareTo(sHashValue) > 0);
		else
			return (sLower.compareTo(sHashValue) > 0) && (sUpper.compareTo(sHashValue) < 0);
	}
}
