package app_kvServer;

import java.util.HashMap;

class Node {
    String key;
    String value;
    int frequency;
    Node next = null;
    Node prev = null;
}

public class LFUCache {

    public LFUCache(int capacity) {
    	// TODO Auto-generated constructor stub
		this.max_size=capacity;
		this.allowed_size=capacity;
        this.min_frequency=0;
        lfu_cache = new HashMap<String, Node>();
        frequency_map = new HashMap<Integer, Node[]>();
        
    }
    
	public int get_capacity()
	{
		return allowed_size;
	}
	
	public boolean inCache(String key)
	{
		return lfu_cache.containsKey(key);
	}
	
	public void clearCache()
	{
		allowed_size = max_size;
		lfu_cache.clear();
	}
	
    public String get(String key) {
    	
    	Node n = lfu_cache.get(key);
        n.prev.next = n.next;
        n.next.prev = n.prev;
        Node[] curPtrs = frequency_map.get(this.min_frequency);
        
        if (curPtrs[0].next == curPtrs[1]) {
            this.min_frequency++;
        }
        
        n.frequency++;
        Node[] ptrs = frequency_map.get(n.frequency);
        if (ptrs == null) {
            Node head = new Node();
            Node tail = new Node();
            head.next = n;
            n.prev = head;
            
            n.next = tail;
            tail.prev = n;
            
            ptrs = new Node[]{head, tail};
            frequency_map.put(n.frequency, ptrs);
        } else {
            Node head = ptrs[0];
            
            Node tmp = head.next;
            head.next = n;
            n.prev = head;
            
            n.next = tmp;
            tmp.prev = n;
        }
        
        return n.value;
    }
    
    public void put(String key, String value) {
        if (this.max_size<=0) {
            return ;
        }
        
        if (lfu_cache.containsKey(key)) {
            Node n = lfu_cache.get(key);
            
            n.value = value;
            
            n.prev.next = n.next;
            n.next.prev = n.prev;
            
            Node[] curPtrs = frequency_map.get(this.min_frequency);
            if (curPtrs[0].next == curPtrs[1]) {
                this.min_frequency++;
            }
            
            n.frequency++;
            Node[] ptrs = frequency_map.get(n.frequency);
            if (ptrs == null) {
                Node head = new Node();
                Node tail = new Node();
                
                head.next = tail;
                tail.prev = head;
                ptrs = new Node[]{head, tail};
                frequency_map.put(n.frequency, ptrs);
            }
            
            Node head = ptrs[0];
            Node tmp = head.next;
            head.next = n;
            n.prev = head;
            
            n.next =tmp;
            tmp.prev = n;
            
            return ;
        }
        
        Node n = new Node();
        n.key = key;
        n.value = value;
        n.frequency = 1;
        lfu_cache.put(key, n);
        this.allowed_size=this.allowed_size-1;
        //evict 
        if (this.allowed_size<0) {
        	//delete first with current minimum frequency
        	//get all minimum frequency
            Node[] ptrs = frequency_map.get(this.min_frequency);
            Node tail = ptrs[1];
            Node least = tail.prev;
            least.prev.next = tail;
            tail.prev = least.prev;
            lfu_cache.remove(least.key);
            this.allowed_size=this.allowed_size+1;
        }
        
        //update new minimum frequency
        this.min_frequency = 1;
        
        if (frequency_map.containsKey(this.min_frequency)) {
        	//when not only one has frequency =1
            Node[] ptrs= frequency_map.get(min_frequency);
            Node head = ptrs[0];
            
            Node temp = head.next;
            head.next = n;
            n.prev = head;
            
            n.next = temp;
            temp.prev = n;
            this.allowed_size=this.allowed_size-1;
        } 
        else {
        	
            Node head = new Node();
            Node tail = new Node();
            head.next = n;
            n.prev = head;
            
            n.next = tail;
            tail.prev = n;
            
            Node[] ptrs = {head, tail};
            frequency_map.put(this.min_frequency, ptrs);
            this.allowed_size=this.allowed_size-1;
        }
    }	
    
	private int allowed_size;
	private int max_size;
	private int min_frequency;
	private HashMap<String, Node> lfu_cache;	
	private HashMap<Integer, Node[]> frequency_map;

}
