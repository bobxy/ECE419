package app_kvClient;

import client.KVCommInterface;
import client.KVStore;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import logger.LogSetup;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import Utilities.Utilities;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import common.KVMessage;
import common.KVMessageC;
import common.KVMessage.StatusType;
import java.time.*;
import org.apache.log4j.Level;

public class KVClient implements IKVClient {
	private static final String PROMPT = "Client> ";
	private static final String HELP = "help";
	private static final String PUT = "put";
	private static final String GET = "get";
	private static final String DELETE = "delete";
	private static final String QUIT = "quit";
	private static final String CONNECT = "connect";
	private static final String SKIP = "skip";
	private static final String TEST = "test";
	private static final String DISCONNECT = "disconnect";
	private static final String TRANSACTION = "transaction";
	private static final String COMMIT = "commit";
	private static final String LOGLEVEL = "logLevel";
	private static final String SUBSCRIBE = "subscribe";
	private static final String UNSUBSCRIBE = "unsubscribe";
	private KVStore KVS;
	private Utilities util;
	private Logger logger = Logger.getRootLogger();
	private HashMap<String, String> ToBeCommitted;
	public HashMap<String, String> Subscriptions;
	private boolean bInTransaction;
	
    @Override
    public void newConnection(String hostname, int port) throws Exception{
    	logger.info("Trying to connect... Host: " + hostname + " Port: " + Integer.toString(port));
    	util = new Utilities();
    	KVS = new KVStore(hostname, port);
    	KVS.connect();
    	ToBeCommitted = new HashMap<String, String>();
    	bInTransaction = false;
    	//KVS.start();
    	logger.info("New connection created. Host: " + hostname + " Port: " + Integer.toString(port));
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
    					if(IsConnected())
    						ValidateReturnedMessage(get(sElements[1]));
    				}
    				else if(sAction.equals(PUT))
    				{
    					if(IsConnected())
    						ValidateReturnedMessage(put(sElements[1], sElements[2]));
    				}
    				else if(sAction.equals(CONNECT))
    					newConnection(sElements[1], Integer.parseInt(sElements[2]));
    				else if(sAction.equals(DISCONNECT) && KVS != null)
    				{
    					KVS.disconnect();
    					KVS = null;
    				}
    				else if(sAction.equals(LOGLEVEL))
    					SetLogLevel(sElements[1]);
    				else if(sAction.equals(SKIP))
    					continue;
    				else if(sAction.equals(TEST))
    					newConnection("localhost", 6666);
    				else if(sAction.equals(TRANSACTION))
    				{
    					if(IsConnected())
    					{
	    					if(InTransaction())
	    						System.out.println("Already in a transaction.");
	    					else
	    					{
	    						bInTransaction = true;
	    						System.out.println("Entered a new transaction.");
	    					}
    					}
    				}
    				else if(sAction.equals(COMMIT))
    				{
    					if(InTransaction())
    					{
    						if(IsConnected())
    						{
	    						bInTransaction = false;
	    						if(commit())
	    							System.out.println("Changes committed. Out of transaction");
	    						else
	    							System.out.println("Nothing to commit. Out of transaction");
    						}
    					}
    					else
    						System.out.println("Not in a transaction.");
    				}
    				else if(sAction.equals(SUBSCRIBE))
    				{
    					subscribe(sElements[1]);
    				}
    				else if(sAction.equals(UNSUBSCRIBE))
    				{
    					unsubscribe(sElements[1]);
    				}
    			}
    			else
    				System.out.println("Error. Type help for instructions.");
    		}
    		if(KVS != null)
    			KVS.disconnect();
    		KVS = null;
    		System.out.println("Quiting..");
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
	    	int nElementCount = sElements.length;
	    	//help, quit, disconnect
	    	if(sCommand.equals(HELP) || sCommand.equals(QUIT) || sCommand.equals(TEST) || sCommand.equals(DISCONNECT) || sCommand.equals(TRANSACTION) || sCommand.equals(COMMIT))
	    	{
	    		String[] ret = {sCommand};
	    		return ret;
	    	}
	    	
	    	//Error
	    	if(nElementCount < 2)
	    		return null;
	    	
	    	String sAction = sElements[0];
	    	String sKey = sElements[1];
	    	
	    	//get command
	    	if((sAction.equals(GET) || sAction.equals(SUBSCRIBE) || sAction.equals(UNSUBSCRIBE)) && nElementCount == 2)
	    	{
	    		String[] ret = {sAction, sKey};
	    		return ret;
	    	}
	    	
	    	//put command
	    	if(sAction.equals(PUT))
	    	{
	    		//error
	    		if(nElementCount < 3)
	    			return null;
	    		
	    		if(nElementCount > 3 && !sElements[2].startsWith("\""))
	    		{
	    			String[] ret = {SKIP};
	    			System.out.println("Error. A key cannot contain any space.");
	    			return ret;
	    		}
	    		
	    		int nValueStart = sCommand.indexOf("\"") + 1;
	    		int nValueEnd = sCommand.lastIndexOf("\"");
	    		int nCommandLength = sCommand.length();
	    		
	    		if(nValueStart >= nCommandLength || nValueEnd != nCommandLength - 1)
	    			return null;
	    		
	    		String sValue = sCommand.substring(nValueStart, nValueEnd);
	    		int nValueLength = sValue.length();
	    		if(nValueLength > 0 && nValueLength <= 120000)
	    		{
	    			String[] ret = {sAction, sKey, sValue};
	    			return ret;
	    		}
	    		else
	    		{
	    			String[] ret = {SKIP};
	    			System.out.println("Error. Value cannot be empty or exceed 120,000 charaters. To delete, please use \"delete\" command or put null as the value.");
	    			return ret;
	    		}
	    	}
	    	
	    	//delete command
	    	if(sAction.equals(DELETE) && nElementCount ==2)
	    	{
	    		String[] ret = {PUT, sKey, ""};
	    		return ret;
	    	}
	    	
	    	//logLevel
	    	if(sAction.equals(LOGLEVEL) && nElementCount == 2)
	    	{
	    		String[] ret = {LOGLEVEL, sKey};
	    		return ret;
	    	}
	    	
	    	//connect command
	    	if(sAction.equals(CONNECT) && nElementCount == 3)
	    	{
	    		String[] ret = {sAction, sKey, sElements[2]};
	    		return ret;
	    	}
    	}catch(Exception e) {
			System.out.println("Error when parsing command!");
			e.printStackTrace();
			System.exit(1);
    	}
    	return null;
    }
    
    private void help(){
    	System.out.println("Please follow the following foramts for the instructions");
    	System.out.println(PUT + " <key>" + " \"<value>\"");
    	System.out.println(GET + " <key>");
    	System.out.println(DELETE + " <key>");
    	System.out.println(CONNECT + " <host>" + " <port>");
    	System.out.println(DISCONNECT);
    	System.out.println(LOGLEVEL + " <level> (one of " + LogSetup.getPossibleLogLevels() + ")");
    	System.out.println(QUIT);
    }
    
    private KVMessage get(String key) throws Exception {
    	if(InTransaction())
    	{
    		if(ToBeCommitted.containsKey(key))
    		{
    			return new KVMessageC(key, ToBeCommitted.get(key), StatusType.GET_SUCCESS);
    		}
    		else
    			return KVS.get(key);
    	}
    	else
    		return KVS.get(key);
    }
    
    private KVMessage put(String key, String value) throws Exception {
    	if(util.InvaildKey(key) || value == null)
    	{
    		KVMessageC message = new KVMessageC(key, value, StatusType.PUT_ERROR);
    		return message;
    	}
    	KVMessage message;
    	if(InTransaction())
    	{
    		ToBeCommitted.put(key, value);
    		message = new KVMessageC(key, value, StatusType.PUT_SUCCESS);
    	}
    	else
    	{
    		message = KVS.put(key, value);
    	}
    	return message;
    }
    
    private void ValidateReturnedMessage(KVMessage message){
    	StatusType code = message.getStatus();
    	if(code == null)
    		return;
    	if(code == StatusType.GET_SUCCESS)
    	{
    		System.out.println("GET SUCCESS. " + message.getKey() + ": " + message.getValue());
    	}
    	else if(code == StatusType.PUT_SUCCESS)
    	{
    		System.out.println("PUT SUCCESS. " + message.getKey() + ": " + message.getValue());
    	}
    	else if(code == StatusType.PUT_UPDATE)
    	{
    		System.out.println("UPDATE SUCCESS. " + message.getKey() + ": " + message.getValue());
    	}
    	else if(code == StatusType.DELETE_SUCCESS)
    	{
    		System.out.println("DELETE SUCCESS. DELETED " + message.getKey());
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
    	else if(code == StatusType.SERVER_STOPPED)
    	{
    		System.out.println("Falied. The server has been stopped.");
    	}
    	else if(code == StatusType.SERVER_WRITE_LOCK)
    	{
    		System.out.println("Falied. The server is busy moving data.");
    	}
    	else if(code == StatusType.SERVER_NOT_RESPONSIBLE)
    	{
    		System.out.println("Falied. Something went wrong.");
    	}
    }
    
    private void SetLogLevel(String sLevel)
    {
    	Level le = LogSetup.GetValidLevel(sLevel);
    	if(le != null)
    	{
    		logger.info("LogLevel set to " + sLevel);
    		logger.setLevel(le);
    	}
    	else
    		System.out.println("Invalid LogLevel.");
    }
    
    private boolean InTransaction()
    {
    	return bInTransaction;
    }
    
    private boolean commit() throws Exception
    {
    	if(ToBeCommitted.isEmpty())
    	{
    		return false;
    	}
    	Set<String> keys = ToBeCommitted.keySet();
    	for(String key : keys)
    	{
    		ValidateReturnedMessage(KVS.put(key, ToBeCommitted.get(key)));
    	}
    	ToBeCommitted.clear();
    	return true;
    }
    
    private void subscribe(String key) throws Exception
    {
    	if(IsConnected())
    	{
    		KVMessage message = get(key);
    		if(message.getStatus() == StatusType.GET_SUCCESS)
    		{
    			String sValue = message.getValue();
    			System.out.println("Subscribed to key: " + key + ". Current value: " + sValue);
    			Subscriptions.put(key, sValue);
    		}
    		else if(message.getStatus() == StatusType.GET_ERROR)
    		{
    			System.out.println("Subscribed to key: " + key + ", which does not exist at this moment.");
    			Subscriptions.put(key, "");
    		}
    	}
    }
    
    private void unsubscribe(String key) throws Exception
    {
    	System.out.println("Unsubscribed to key: " + key + ".");
    	Subscriptions.remove(key);
    }
    
    private boolean IsConnected()
    {
    	boolean bConnected = (KVS != null);
    	if(!bConnected)
    		System.out.println("Error. Please connect to a server.");
    	return bConnected;
    }
    
    public static void main(String[] args) throws Exception{
    	BasicConfigurator.configure();
    	new LogSetup("logs/client.log", Level.ALL);
    	KVClient cli = new KVClient();
    	SubscriptionService SS = new SubscriptionService(cli);
    	SS.start();
    	cli.run();
    }
}

