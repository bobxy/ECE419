package testing;

import java.net.ConnectException;
import java.net.UnknownHostException;

import javax.xml.ws.Response;

import org.junit.Test;

import common.messages.KVMessage;
import common.messages.KVMessage.StatusType;
import client.KVStore;
import junit.framework.TestCase;

public class AdditionalTest extends TestCase {
	
	// TODO add your test cases, at least 10
	
	private KVStore kvClient;
	
	public void setUp() {
		kvClient = new KVStore("localhost", 6666);
		try {
			kvClient.connect();
		} catch (Exception e) {
		}
	}

	public void tearDown() {
		kvClient.disconnect();
	}
	
	@Test
	public void testPutEmptyStringFirst() {
		String key = "PESF";
		String value = "";
		KVMessage response = null;
		Exception ex = null;
		
		try {
			response = kvClient.put(key, value);
		} catch (Exception e) {
			ex = e;
		}
		
		assertTrue(ex == null && response.getStatus() == StatusType.DELETE_ERROR);

	}
	
	@Test
	public void testPutNullFirst() {
		String key = "BOB";
		String value = "null";
		KVMessage response = null;
		Exception ex = null;
		
		try {
			response = kvClient.put(key, value);
		} catch (Exception e){
			ex = e;
		}
		
		assertTrue(ex == null && response.getStatus() == StatusType.DELETE_ERROR);
		
	}
	
	@Test
	public void testPutGetValueWithSpaces() {
		String key = "testPVWS";
		String value = "a b c d";
		KVMessage response = null;
		Exception ex = null;
		
		try {
			response = kvClient.put(key, value);
		} catch (Exception e) {
			ex = e;
		}
		
		assert(ex == null && response.getStatus() == StatusType.PUT_SUCCESS);
		
		try {
			response = kvClient.get(key);
		} catch (Exception e) {
			ex = e;
		}
		
		assert(ex == null && response.getStatus() == StatusType.GET_SUCCESS && response.getValue().equals("a b c d"));
	}
	
	@Test
	public void testPutDeletePut() {
		String key = "Tony";
		String value = "Fang";
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
			}catch (Exception e){
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
	
	@Test
	public void testPutDeleteGet () {
		String key = "testPDG";
		String value = "PDG";
		KVMessage response= null;
		Exception ex = null;
		
		try{
			response = kvClient.put(key, value);
		} catch (Exception e) {
			ex = e;
		}
		
		assertTrue(ex == null && response.getStatus() == StatusType.PUT_SUCCESS);
		
		try{
			response = kvClient.put(key, "null");
		} catch (Exception e) {
			ex = e;
		}
		
		assertTrue(ex == null && response.getStatus() == StatusType.DELETE_SUCCESS);
		
		try{
			response = kvClient.get(key);
		} catch (Exception e) {
			ex = e;
		}
		
		assertTrue(ex == null && response.getStatus() == StatusType.GET_ERROR);
		
	}
	
	
	@Test
	public void testPutDeleteDelete (){
		String key = "test";
		String value = "PDD";
		KVMessage response = null;
		Exception ex = null;
		
		try{
			response = kvClient.put(key, value);
		} catch (Exception e){	
			ex = e;
		}
		
		assertTrue(ex == null && response.getStatus() == StatusType.PUT_SUCCESS);
		
		try{
			response = kvClient.put(key, "null");
		} catch (Exception e){	
			ex = e;
		}
		
		assertTrue(ex == null && response.getStatus() == StatusType.DELETE_SUCCESS);

		try{
			response = kvClient.put(key, "null");
		} catch (Exception e){	
			ex = e;
		}
		
		assertTrue(ex == null && response.getStatus() == StatusType.DELETE_ERROR);
		
	}
	
	@Test
	public void testGetDisconnected () {
		
		String key = "LAI";
		String value = "GD";
		
		KVMessage response = null;
		Exception ex = null;
		
		try{
			response = kvClient.put(key, value);
		} catch (Exception e) {
			ex = e;
		}

		assertTrue(ex == null && response.getStatus() == StatusType.PUT_SUCCESS);

		kvClient.disconnect();
		
		try{
			response = kvClient.get(key);
		} catch (Exception e) {
			ex = e;
		}
		
		assertNotNull(ex);
		
	}
	
	
	@Test
	public void testWeirdKey (){
		String key = "()!@#$%^&*";
		String value = "WK";
		KVMessage response = null;
		Exception ex = null;
		
		try{
			response = kvClient.put(key, value);
		} catch (Exception e) {
			ex = e;
		}
		
		assertTrue(ex == null && response.getStatus() == StatusType.PUT_SUCCESS);
		
		try{
			response = kvClient.get(key);
		} catch (Exception e) {
			ex = e;
		}
		
		assertTrue(ex == null && response.getStatus() == StatusType.GET_SUCCESS);

	}
	
/*	@Test
	public void testPutKeyWithSpaces (){
		String key = "to ny";
		String value = "tPKWS";
		KVMessage response = null;
		Exception ex = null;
		
		try{
			response = kvClient.put(key, value);
		} catch (Exception e) {
			ex = e;
		}
		
		assertTrue(ex == null && response.getStatus() == StatusType.PUT_ERROR);
		
	}*/
	
	public void testMultipleClientConnections() {
		
		Exception ex = null;
		
		KVStore kvClient1 = new KVStore("localhost", 6666);
		try {
			kvClient1.connect();
		} catch (Exception e) {
			ex = e;
		}	
		
		assertNull(ex);
		
		KVStore kvClient2 = new KVStore("localhost", 6666);
		try {
			kvClient2.connect();
		} catch (Exception e) {
			ex = e;
		}	
		
		assertNull(ex);
		
		kvClient1.disconnect();
		kvClient2.disconnect();
	}
	
	public void testIncorrectPort() {
		Exception ex = null;
		KVStore kvClient = new KVStore("localhost", 6665);
		
		try {
			kvClient.connect();
		} catch (Exception e) {
			ex = e; 
		}
		
		assertTrue(ex instanceof ConnectException);
	}
	
	
}
