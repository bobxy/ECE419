package common;
import java.io.*;
import java.util.*;
import common.ServerConfiguration;

public class ServerConfigurations implements Serializable{
	private static final long serialVersionUID = 1L;
	private HashMap<String, ServerConfiguration> ServerInfo;
	private ArrayList<String> ServerList;
	
	public ServerConfigurations()
	{
		ServerInfo = new HashMap<String, ServerConfiguration>();
		ServerList = new ArrayList<String>();
	}
	
	public void AddServers(ServerConfiguration[] config)
	{
		int nSize = config.length;
		for(int i = 0; i < nSize; i++)
		{
			String sHashValue = config[i].GetHashValue();
			ServerInfo.put(sHashValue, config[i]);
			ServerList.add(sHashValue);
		}
		Collections.sort(ServerList);
	}
	
	public void RemoveServers()
	{
		ServerList.clear();
		ServerInfo.clear();
	}
	
	public ServerConfiguration FindServerForKey(String sKey)
	{
		String sHashValue = sKey; // = ...;
		int nSize = ServerList.size();
		if(nSize <= 0)
			return null;
		for(int i = 0; i < nSize; i++)
		{
			if(sHashValue.compareTo(ServerList.get(i)) > 0)
				return 	ServerInfo.get(sKey);
		}
		return ServerInfo.get(ServerList.get(0));
	}
	
	public boolean IsEmpty()
	{
		return ServerList.isEmpty();
	}
}
