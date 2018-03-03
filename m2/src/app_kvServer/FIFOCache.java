package app_kvServer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Queue;

public class FIFOCache{	

	public FIFOCache(int capacity) {
		// TODO Auto-generated constructor stub
		this.max_size=capacity;
		this.allowed_size=capacity;
		fifo_cache = new HashMap<String, String>();
		fifo_list = new LinkedList<String>();

	}
	
	public int get_capacity()
	{
		return this.allowed_size;
	}
	
	public boolean inCache(String key)
	{
		return fifo_cache.containsKey(key);
	}
	
	public void clearCache()
	{
		this.allowed_size = this.max_size;
		fifo_list.clear();
		fifo_cache.clear();
	}
	public void put(String key, String value)
	{
        if (this.max_size<=0) {
            return ;
        }
		
		if(fifo_cache.containsKey(key))
		{
			//already in the queue, only need to update the value
			fifo_cache.put(key, value);
			//change the order in the queue
			fifo_list.remove(key);
			fifo_list.addLast(key);
		}
		else
		{
			if(this.allowed_size>0)
			{
				//don't need to evict
				fifo_cache.put(key, value);
				fifo_list.addLast(key);
				//update size
				this.allowed_size = this.allowed_size-1;
			}
			else
			{
				//evict first one
				String removed_key=fifo_list.poll();
				fifo_cache.remove(removed_key);
				
				//add new element;
				fifo_cache.put(key, value);
				fifo_list.addLast(key);
			}
		}

		
	}
	
	public String get(String key)
	{
		return fifo_cache.get(key);
	}
	

	//size of queue
	private int allowed_size;
	private int max_size;

	private LinkedList<String> fifo_list;
	//cache<key,value>
	private HashMap<String, String> fifo_cache;	

}
