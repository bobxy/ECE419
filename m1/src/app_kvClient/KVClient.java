package app_kvClient;

import client.KVCommInterface;
import client.KVStore;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.HashSet;
import java.util.Set;
import logger.LogSetup;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import common.messages.KVMessage;
import common.messages.KVMessage.StatusType;

public class KVClient implements IKVClient {
	private static final String PROMPT = "Client> ";
	private static final String HELP = "help";
	private static final String PUT = "put";
	private static final String GET = "get";
	private static final String DELETE = "delete";
	private static final String QUIT = "quit";
	private static final String CONNECT = "connect";
	private KVStore KVS;
	
	
    @Override
    public void newConnection(String hostname, int port) throws Exception{
    	KVS = new KVStore(hostname, port);
    	KVS.connect();
    	KVS.start();
    	System.out.println(PROMPT + "New connection created. Host: " + hostname + " Port: " + Integer.toString(port));
    	return;
    }

    @Override
    public KVCommInterface getStore(){
        return KVS;
    }
    
    public void run(){
    	try
    	{
    		while(true)
    		{
    			System.out.print(PROMPT);
    			BufferedReader stdin = new BufferedReader(new InputStreamReader(System.in));
    			String cmdLine = stdin.readLine();
    			String[] sElements = Parse(cmdLine);
    			if(sElements != null)
    			{
    				String sAction = sElements[0];
    				if(sAction.equals(HELP))
    					help();
    				else if(sAction.equals(QUIT))
    					break;
    				else if(sAction.equals(GET))
    				{
    					if(KVS != null)
    						ValidateReturnedMessage(get(sElements[1]));
    					else
    						System.out.println(PROMPT + "Error. Please connect to a server.");
    				}
    				else if(sAction.equals(PUT))
    				{
    					if(KVS != null)
    						ValidateReturnedMessage(put(sElements[1], sElements[2]));
    					else
    						System.out.println(PROMPT + "Error. Please connect to a server.");
    				}
    				else if(sAction.equals(CONNECT))
    					newConnection(sElements[1], Integer.parseInt(sElements[2]));
    			}
    			else
    				System.out.println(PROMPT + "Error. Type help for instructions.");
    		}
    		if(KVS != null)
    			KVS.disconnect();
    	}
    	catch (Exception e) {
			System.out.println("Error!");
			e.printStackTrace();
			System.exit(1);
		}
    }
    
    private String[] Parse(String sCommand)
    {
    	try
    	{
	    	sCommand = sCommand.trim();
	    	String[] sElements = sCommand.split(" ");
	    	
	    	//help or quit
	    	if(sCommand.equals(HELP) || sCommand.equals(QUIT))
	    	{
	    		String[] ret = {sCommand};
	    		return ret;
	    	}
	    	
	    	//Error
	    	if(sElements.length < 2)
	    		return null;
	    	
	    	String sAction = sElements[0];
	    	String sKey = sElements[1];
	    	
	    	//get command
	    	if(sAction.equals(GET))
	    	{
	    		String[] ret = {sAction, sKey};
	    		return ret;
	    	}
	    	
	    	//put command
	    	if(sAction.equals(PUT))
	    	{
	    		//error
	    		if(sElements.length < 3)
	    			return null;
	    		
	    		int nValueStart = sCommand.indexOf("\"") + 1;
	    		int nValueEnd = sCommand.indexOf("\"", nValueStart);
	    		int nCommandLength = sCommand.length();
	    		
	    		if(nValueStart >= nCommandLength || nValueEnd != nCommandLength - 1)
	    			return null;
	    		
	    		String sValue = sCommand.substring(nValueStart, nValueEnd);
	    		if(sValue.length() > 0)
	    		{
	    			String[] ret = {sAction, sKey, sValue};
	    			return ret;
	    		}
	    		else
	    		{
	    			System.out.println(PROMPT + "Value cannot be empty. To delete, please use \"delete\" command.");
	    			return null;
	    		}
	    	}
	    	
	    	//delete command
	    	if(sAction.equals(DELETE))
	    	{
	    		String[] ret = {PUT, sKey, ""};
	    		return ret;
	    	}
	    	
	    	if(sAction.equals(CONNECT) && sElements.length >= 3)
	    	{
	    		String[] ret = {sAction, sKey, sElements[2]};
	    		return ret;
	    	}
    	}catch(Exception e) {
			System.out.println("Error!");
			e.printStackTrace();
			System.exit(1);
    	}
    	return null;
    }
    
    private void help(){
    	System.out.print(PROMPT + "Please follow the following foramts for the instructions");
    	System.out.print(PROMPT + PUT + " key" + " \"value\"");
    	System.out.print(PROMPT + GET + " key");
    	System.out.print(PROMPT + DELETE + " key");
    	System.out.print(PROMPT + CONNECT + " host" + " port");
    	System.out.print(PROMPT + QUIT);
    }
    
    private KVMessage get(String key) throws Exception {
    	KVMessage message = KVS.get(key);
    	return message;
    }
    
    private KVMessage put(String key, String value) throws Exception {
    	KVMessage message = KVS.put(key, value);
    	return message;
    }
    
    private void ValidateReturnedMessage(KVMessage message){
    	StatusType code = message.getStatus();
    	if(code == null)
    		return;
    	if(code == StatusType.GET_SUCCESS)
    	{
    		System.out.println("GET SUCCESS " + message.getKey() + ": " + message.getValue());
    	}
    	else if(code == StatusType.PUT_SUCCESS)
    	{
    		System.out.println("PUT SUCCESS " + message.getKey() + ": " + message.getValue());
    	}
    	else if(code == StatusType.PUT_UPDATE)
    	{
    		System.out.println("UPDATE SUCCESS " + message.getKey() + ": " + message.getValue());
    	}
    	else if(code == StatusType.DELETE_SUCCESS)
    	{
    		System.out.println("DELETE SUCCESS " + message.getKey());
    	}
    	else if(code == StatusType.GET_ERROR)
    	{
    		System.out.println("GET ERROR");
    	}
    	else if(code == StatusType.PUT_ERROR)
    	{
    		System.out.println("PUT ERROR");
    	}
    	else if(code == StatusType.DELETE_ERROR)
    	{
    		System.out.println("DELETE ERROR");
    	}
    }
    
    public static void main(String[] args) throws Exception{
    	KVClient cli = new KVClient();
    	cli.run();
    }
}
