package common;
import java.io.*;
import java.security.NoSuchAlgorithmException;
import java.util.*;

import common.ServerConfiguration;
import Utilities.Utilities;
public class ServerConfigurations implements Serializable{
	private static long serialVersionUID;
	private static HashMap<String, ServerConfiguration> ServerInfo;
	private Utilities util;
	private SortedSet<String>MD5Set;
	public ServerConfigurations()
	{
		util = new Utilities();
		serialVersionUID = 1L;
		ServerInfo = new HashMap<String, ServerConfiguration>();
		MD5Set = new TreeSet<>();
	}
	
	public void AddServers(ServerConfiguration[] config)
	{
		int nSize = config.length;
		for(int i = 0; i < nSize; i++)
		{
			String sHashValue = config[i].GetHashValue();
			ServerInfo.put(sHashValue, config[i]);
			MD5Set.add(sHashValue);
		}
	}
	
	public ServerConfiguration FindServerByServerNameHash(String MD5Hash)
	{
		return ServerInfo.get(MD5Hash);
	}
	public void AddServer(ServerConfiguration config)
	{
		ServerInfo.put(config.GetHashValue(),config);
		MD5Set.add(config.GetHashValue());
	}
	
	public ServerConfiguration FindNextHigher(String MD5Hash)
	{
		SortedSet<String> tempSet=MD5Set.tailSet(MD5Hash);
		String res="";
		tempSet.remove(MD5Hash);
		res=tempSet.first();
		return ServerInfo.get(res);
	}
	public void RemoveServers()
	{
		ServerInfo.clear();
		MD5Set.clear();
	}
	
	public void RemoveSpecificServer(String MD5Hash)
	{
		ServerInfo.remove(MD5Hash);
		MD5Set.remove(MD5Hash);
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
