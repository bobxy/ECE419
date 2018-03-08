package common;
import java.io.*;
import java.util.*;
import common.ServerConfiguration;

public class ServerConfigurations implements Serializable{
	private static final long serialVersionUID = 1L;
	private HashMap<String, ServerConfiguration> ServerInfo;
	
	public ServerConfigurations()
	{
		ServerInfo = new HashMap<String, ServerConfiguration>();
	}
	
	public void AddServers(ServerConfiguration[] config)
	{
		int nSize = config.length;
		for(int i = 0; i < nSize; i++)
		{
			String sHashValue = config[i].GetHashValue();
			ServerInfo.put(sHashValue, config[i]);
		}
	}
	
	public void RemoveServers()
	{
		ServerInfo.clear();
	}
	
	public ServerConfiguration FindServerForKey(String sKey)
	{
		String sHashValue = sKey; // = ...;
		for(ServerConfiguration config : ServerInfo.values())
		{
			if(config.IsResponsible(sHashValue))
				return config;
		}
		return null;
	}
	
	public boolean IsEmpty()
	{
		return ServerInfo.isEmpty();
	}
}
