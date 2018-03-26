import java.util.ArrayList;

import client.KVStore;

import app_kvClient.KVClient;
import app_kvServer.KVServer;

import java.time.Duration;
import java.time.Instant;

public class PerformanceTest {

	/**
	 * @param args
	 * Different number of clients connected to the service (e.g., 1, 5, 20, 50, 100)
		Different number of storage servers participating in the storage service (e.g., 1, 5, 10, 50, 100)
		Different KV server configurations (cache sizes, strategies)
	 * @throws Exception 

	 */
	
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		int number_clients=100;
		int number_request=10;
		
		ArrayList<KVStore> clientList=new ArrayList<KVStore>();
		System.out.println("goes to performance test");
		
		for(int i=0;i<number_clients;i++)
		{
			KVStore newclient=new KVStore("ug153",62100);
			newclient.connect();
			clientList.add(newclient);
		}
		System.out.println("after create clients");
		
		Instant start_put = Instant.now();
		System.out.println("after instant now");
		for(int i=0;i<clientList.size();i++)
		{
			System.out.println("current client number: "+i);
			for(int j=0;j<number_request;j++)
			{
				System.out.println("number of request: "+j);
				clientList.get(i).put("client"+i+"_"+j, "This is a message to test performance, this is a very long message");
			}
		}
		Instant end_put = Instant.now();
		Duration timeElapsed = Duration.between(start_put, end_put);
		System.out.println("Time taken put : "+ timeElapsed.toMillis() +" milliseconds");
		
		Instant start_get = Instant.now();
		for(int i=0;i<clientList.size();i++)
		{
			for(int j=0;j<number_request;j++)
			{
				clientList.get(i).get("client"+i+"_"+j);
			}
		}
		Instant end_get = Instant.now();
		timeElapsed = Duration.between(start_get, end_get);
		System.out.println("Time taken get: "+ timeElapsed.toMillis() +" milliseconds");
	}

}
