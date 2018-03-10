package common;
import java.io.*;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import common.ServerConfiguration;
import Utilities.Utilities;
public class ServerConfigurations implements Serializable{
	private static final long serialVersionUID = 1L;
	private static HashMap<String, ServerConfiguration> ServerInfo = new HashMap<String, ServerConfiguration>();
	private Utilities util = new Utilities();
	
	public ServerConfigurations()
	{
		
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
	
	public ServerConfiguration FindServerForKey(String sKey) throws UnsupportedEncodingException, NoSuchAlgorithmException
	{
		String sHashValue = util.cHash(sKey);
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
