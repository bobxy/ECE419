package app_kvServer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Queue;

public class FIFOCache {	

	public FIFOCache(int capacity) {
		// TODO Auto-generated constructor stub
		size=capacity;
	}
	
	void put(String key, String value) throws IOException
	{
		diskOperation mydisk=new diskOperation();
		//hash to determine which file to write
		
		String file_path="";
		int file_num=file_number_hash(key,file_num_path.size());
		
		file_path=file_num_path.get(file_num);
		
		int file_size=file_file_size.get(file_path);
		
		int new_size=mydisk.put_disk(file_path,file_size,key, value);
		
		file_file_size.put(file_path, new_size);
	}
	
	String get(String key)
	{
		String res=fifo_cache.get(key);
		if(res !=null)
		{
			return res;
		}
		else
		{
			//check queue size
			String queue_key=
		}
	}
	
	public int file_number_hash(String key,Integer filenums)
	{
		int hash = 7;
		for (int i = 0; i < key.length(); i++) {
		    hash = hash*31 + key.charAt(i);
		}
		return hash%filenums;
	}
	//size of queue
	private int size;
	//queue<key>
	private Queue<String> fifo_queue = new LinkedList<String>();
	
	//cache<key,value>
	private HashMap<String, String> fifo_cache = new HashMap<String, String>();	
	
	//use an array and hash map to store file to file size info. array for random
	
	private HashMap<String, Integer> file_file_size = new HashMap<String, Integer>();
	
	private ArrayList<String> file_num_path = new ArrayList<String>();
}
