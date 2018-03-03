package app_kvServer;

import java.util.HashMap;

public class Cache {

	FIFOCache fifo;
	LRUCache lru;
	LFUCache lfu;
	public Cache(IKVServer.CacheStrategy CacheStrategy, int cacheSize) {
		fifo = null;
		lru = null;
		lfu = null;
		if(CacheStrategy == IKVServer.CacheStrategy.FIFO)
			fifo = new FIFOCache(cacheSize);
		else if(CacheStrategy == IKVServer.CacheStrategy.LRU)
			lru = new LRUCache(cacheSize);
		else if(CacheStrategy == IKVServer.CacheStrategy.LFU)
			lfu = new LFUCache(cacheSize);
		
	}
	
	public boolean inCache(String key)	
	{
		if(fifo != null)
			return fifo.inCache(key);
		if(lru != null)
			return lru.inCache(key);
		if(lfu != null)
			return lfu.inCache(key);
		return false;
	}
		
	
	public void clearCache()
	{
		if(fifo != null)
			fifo.clearCache();
		if(lru != null)
			lru.clearCache();
		if(lfu != null)
			lfu.clearCache();
	}
	
	public String get(String key) 
	{
		if(fifo != null)
			return fifo.get(key);
		if(lru != null)
			return lru.get(key);
		if(lfu != null)
			return lfu.get(key);
		return "";
    }

	public void put(String key, String value)
	{
		if(fifo != null)
			fifo.put(key, value);
		if(lru != null)
			lru.put(key, value);
		if(lfu != null)
			lfu.put(key, value);
	}
}
