package app_kvClient;

import java.util.HashMap;
import java.util.Set;

import common.KVMessage;
import common.KVMessage.StatusType;

import client.KVStore;

public class SubscriptionService implements Runnable{
	private KVClient client;
	private KVStore kvs;
	private HashMap<String, String> Subscriptions;
	boolean free;
	public SubscriptionService(KVClient cli)
	{
		client = cli;
		kvs = null;
		Subscriptions = new HashMap<String, String>();
		free = true;
	}
	public void run()
	{
		try
		{
			while(true)
			{
				UpdateKVS();
				Enter();
				Set<String> subscription = Subscriptions.keySet();
				for(String key : subscription)
				{
					KVMessage message = kvs.get(key);
					if(message.getStatus() == StatusType.GET_SUCCESS)
					{
						String sNewValue = message.getValue();
						String sOldValue = Subscriptions.get(key);
						if(!sNewValue.equals(sOldValue))
						{
							Notification(key, sOldValue, sNewValue);
							Subscriptions.put(key, sNewValue);
						}
					}
					else if(message.getStatus() == StatusType.GET_ERROR)
					{
						System.out.println("The key-value pair with key <" + key + "> has been removed.");
						Subscriptions.put(key, "");
					}
				}
				Exit();
				Thread.sleep(1000);
			}
		}catch (Exception e) {
			e.printStackTrace();
		}
	}
	private void UpdateKVS() throws Exception
	{
		while(true)
		{
			KVStore client_kvs = (KVStore) client.getStore();
			if(client_kvs != null)
			{
				int nClientPort = client_kvs.portnum;
				String sClientHost = client_kvs.addr;
				if(kvs == null || nClientPort != kvs.portnum || !sClientHost.equals(kvs.addr))
				{
					kvs = new KVStore(sClientHost, nClientPort);
					kvs.connect();
				}
				break;
			}
		}
	}
	
	private void Notification(String sKey, String sOldValue, String sNewValue)
	{
		System.out.println();
		if(sOldValue.isEmpty())
			System.out.println("(" + sKey + ", " + sNewValue + ") added.");
		else
			System.out.println("(" + sKey + ", " + sOldValue + ") changed to (" + sKey + ", " + sNewValue + ").");
		System.out.print("Client> ");
	}
	
	private void Enter()
	{
		while(!free);
		free = false;
	}
	
	private void Exit()
	{
		free = true;
	}
	
	public void Subscribe(String key, String value, boolean bSubscribe)
	{
		Enter();
		if(bSubscribe)
		{
			Subscriptions.put(key, value);
		}
		else
		{
			if(Subscriptions.containsKey(key))
				Subscriptions.remove(key);
		}
		Exit();
		return;
	}
}
