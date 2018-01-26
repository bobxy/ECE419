package testing;

import java.net.UnknownHostException;

import client.KVStore;

import junit.framework.TestCase;


public class ConnectionTest extends TestCase {

	
	public void testConnectionSuccess() throws Exception {
		
		Exception ex = null;

	
		try {
			KVStore kvClient = new KVStore("localhost", 50000);
			//kvClient.connect();
		} catch (Exception e) {
			ex = e;
		}	
		
		assertNull(ex);
	}
	
	
	public void testUnknownHost() throws Exception {
		Exception ex = null;
		//KVStore kvClient = new KVStore("unknown", 50000);
		
		try {
			KVStore kvClient = new KVStore("unknown", 50000);
			//kvClient.connect();
		} catch (Exception e) {
			ex = e; 
		}
		
		assertTrue(ex instanceof UnknownHostException);
	}
	
	
	public void testIllegalPort() throws Exception {
		Exception ex = null;
		//KVStore kvClient = new KVStore("localhost", 123456789);
		
		try {
			KVStore kvClient = new KVStore("localhost", 123456789);
			//kvClient.connect();
		} catch (Exception e) {
			ex = e; 
		}
		
		assertTrue(ex instanceof IllegalArgumentException);
	}
	
	

	
}

