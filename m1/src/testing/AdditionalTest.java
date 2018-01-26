package testing;

import org.junit.Test;

import common.messages.KVMessage;
import common.messages.KVMessage.StatusType;

import client.KVStore;

import junit.framework.TestCase;

public class AdditionalTest extends TestCase {
	
	// TODO add your test cases, at least 3
	
	private KVStore kvClient;
	
	public void setUp() throws Exception {
		
		kvClient = new KVStore("localhost", 50000);
		
		try {
			kvClient.connect();
			System.out.println("client connected");
			kvClient.start();
			System.out.println("thread started");
		} catch (Exception e) {
		}
	}

	public void tearDown() {
		kvClient.disconnect();
	}
	
	@Test
	public void PutDeletePut() {
		String key = "foo2";
		String value = "bar2";
		KVMessage response = null;
		Exception ex = null;

		try {
			response = kvClient.put(key, value);
		} catch (Exception e) {
			ex = e;
		}
		assertTrue(ex == null && response.getStatus() == StatusType.PUT_SUCCESS);

		try {
			response = kvClient.put(key, "null");
			
		} catch (Exception e) {
			ex = e;
		}

		assertTrue(ex == null && response.getStatus() == StatusType.DELETE_SUCCESS);
		
		try {
			response = kvClient.put(key, value);
		} catch (Exception e) {
			ex = e;
		}
		assertTrue(ex == null && response.getStatus() == StatusType.PUT_SUCCESS);
		
	}
}
