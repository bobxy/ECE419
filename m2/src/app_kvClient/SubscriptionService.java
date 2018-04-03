package app_kvClient;

import java.util.Set;

import common.KVMessage;
import common.KVMessage.StatusType;

import client.KVStore;

public class SubscriptionService extends Thread{
	private KVClient client;
	private KVStore kvs;
	public SubscriptionService(KVClient cli)
	{
		client = cli;
		kvs = null;
	}
	public void run()
	{
		try
		{
			while(true)
			{
				UpdateKVS();
				Set<String> subscription = client.Subscriptions.keySet();
				for(String key : subscription)
				{
					KVMessage message = kvs.get(key);
					if(message.getStatus() == StatusType.GET_SUCCESS)
					{
						String sNewValue = message.getValue();
						String sOldValue = client.Subscriptions.get(key);
						if(!sNewValue.equals(sOldValue))
						{
							Notification(key, sOldValue, sNewValue);
							client.Subscriptions.put(key, sNewValue);
						}
					}
					else if(message.getStatus() == StatusType.GET_ERROR)
					{
						System.out.println("The key-value pair with key <" + key + "> has been removed.");
						client.Subscriptions.put(key, "");
					}
				}
				Thread.sleep(10000);
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
		if(sOldValue.isEmpty())
			System.out.println("(" + sKey + ", " + sNewValue + ") added.");
		else
			System.out.println("(" + sKey + ", " + sOldValue + ") changed to (" + sKey + ", " + sNewValue + ").");
		System.out.println("Client> ");
	}
}
