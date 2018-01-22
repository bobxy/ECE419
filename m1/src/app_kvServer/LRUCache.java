package app_kvServer;


import java.util.HashMap;
import java.util.List;

class DLinkNode{
    String key;
    String value;
    DLinkNode prev;
    DLinkNode next;    
}

public class LRUCache {

	public LRUCache(int capacity) {
		// TODO Auto-generated constructor stub
		this.max_size=capacity;
		this.allowed_size=this.max_size;
		lru_cache = new HashMap<String, DLinkNode>();
	}
	
	public int get_capacity()
	{
		return this.allowed_size;
	}
	
	public boolean inCache(String key)	
	{
		return lru_cache.containsKey(key);
	}
	
	public void clearCache()
	{
		this.allowed_size = max_size;
		lru_cache.clear();
		head=null;
		tail=null;
		
	}

	
	public String get(String key)
	{
        DLinkNode node = lru_cache.get(key);
 
        this.moveToHead(node);
        return node.value;
	}
    
    public void put(String key, String value) {
        if (this.max_size<=0) {
            return ;
        }
        DLinkNode node = lru_cache.get(key);
        if(node == null){
        	//new to cache
            DLinkNode newNode = new DLinkNode();
            newNode.key = key;
            newNode.value = value;
            
            this.lru_cache.put(key, newNode);
            this.addNode(newNode);
            this.allowed_size=this.allowed_size-1;
            
            if(this.allowed_size <0){
                DLinkNode last = this.popLast();
                this.lru_cache.remove(last.key);
                allowed_size=allowed_size+1;
            }
        }
        else{
            node.value = value;
            this.moveToHead(node);
        }        
    }
    
    private void addNode(DLinkNode node){
        node.prev = head;
        node.next = head.next;
        head.next.prev = node;
        head.next = node;        
    }
    
    private void removeNode(DLinkNode node){
        DLinkNode prev = node.prev;
        DLinkNode next = node.next;
        node.prev.next = next;
        node.next.prev = prev;        
    }
   
    private void moveToHead(DLinkNode node){
        removeNode(node);
        addNode(node);
    }
    
    private DLinkNode popLast(){
        DLinkNode last = tail.prev;
        removeNode(last);
        return last;
    }
    
    private HashMap<String, DLinkNode> lru_cache;
    private int max_size;
    private int allowed_size;
    private DLinkNode head;
    private DLinkNode tail;
}



