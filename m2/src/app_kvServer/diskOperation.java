package app_kvServer;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.locks.ReentrantLock;

import Utilities.Utilities;

public class diskOperation {

	public diskOperation(String filename) {
		// TODO Auto-generated constructor stub
		//populate lookup
		myutilities=new Utilities();
		lookup_table = new HashMap<String, Integer>();
		myFile = new FileSystem();
		myFile.insert_file(filename);
		MD5Set = new TreeSet<>();
		HashKey=new HashMap<String,String>();
		WriterLock=new ReentrantLock();
	}
	
	public boolean inStorage(String key)
	{
		if(lookup_table.containsKey(key))
		{
			if(lookup_table.get(key)>=0)
			{
				return true;
			}
		}
		return false;
	}
	
	public void load_file_path(String file_path)
	{
		myFile.insert_new_file(file_path);
	}
	
	public void load_lookup_table() throws IOException, NoSuchAlgorithmException
	{
		ArrayList<String> myfiles=myFile.get_all_file_path();
		for(int i=0;i<myfiles.size();i++)
		{
			int position=0;
			String file_path=myfiles.get(i);
			int total_length=0;
			while(position<myFile.get_file_size(file_path))
			{
				boolean isvalid=get_EI_valid(file_path,position);
				int key_length=get_EI_key_length(file_path,position);
				int value_length=get_EI_value_length(file_path,position);
				total_length=0;
				total_length=6+key_length+value_length;
				String key=get_key(file_path,position,key_length);
				if(isvalid)
				{
					//update in lookup table
					lookup_table.put(key, position);
					//first compute the md5 hash for the key
					String MD5Hash=myutilities.cHash(key);
					//update in MD5 sorted set
					MD5Set.add(MD5Hash);
					HashKey.put(MD5Hash, key);
				}
				position=total_length+position;
			}
			
		}
	}
	
    public String get(String key) throws IOException{
        	String res="";
        	int position=0;
        	position=lookup_table.get(key);
        	String file_path=myFile.get_file_path(key);
        		
        	int read_length=get_EI_value_length(file_path,position);
        	int key_length=get_EI_key_length(file_path,position);
        	int offset=6;
        	res=new String(myutilities.mmap_read(file_path,position+offset+key_length,read_length));
        	return res;
	}
    
	public void put(String key, String value) throws IOException, NoSuchAlgorithmException{
		WriterLock.lock();
		String file_path=myFile.get_file_path(key);
		int file_size=myFile.get_file_size(file_path);
		//append to the end of file
		int new_file_size=0;
		String ei="";
		if(lookup_table.containsKey(key))
		{
			//need to set invalid for old key value
			int position=lookup_table.get(key);
			lookup_table.remove(key);
			set_EI_invalid(file_path,position);
			//remove from HashKey,KeyHash maps
			String tempMD5=myutilities.cHash(key);

			MD5Set.remove(tempMD5);
			HashKey.remove(tempMD5);
		}
		if(value.length() == 0)
		{
			return;
		}
		
		//create ei;
		ei=ei+(char)1; //valid =1;
		ei=ei+(char)key.length();
		ei=ei+Utilities.encode_128_value_length(value.length());
		String res=ei+key+value;
		int position=file_size;
		//write to file
		myutilities.mmap_write(file_path, position, res);
		//update lookup table
		lookup_table.put(key, position);
		
		//add md5 to md5set,hashkey table
		String tempMD5=myutilities.cHash(key);
		MD5Set.add(tempMD5);
		HashKey.put(tempMD5, key);
		
		
		new_file_size=file_size+res.length();
		myFile.update_file_size(file_path, new_file_size);
		
		WriterLock.unlock();
		return;
			
	}

	public void clearStorage(){
		//clean up lookup table
		lookup_table.clear();
		//clean up file system
		myFile.clearFile();
		MD5Set.clear();
		HashKey.clear();
	}
	
	// a function to get subset of key value pairs given a hash range
	public ArrayList<KeyValuePair> get_subset(String lowerbound,String upperbound) throws IOException
	{
		ArrayList<KeyValuePair> res= new ArrayList<KeyValuePair>();
		if(lowerbound.compareTo(upperbound)>0)
		{
			String zero="";
			String FF="";
			for(int i=0;i<32;i++)
			{
				zero+="0";
				FF+="F";
			}
			SortedSet<String> temp=MD5Set.subSet(zero, upperbound);
			SortedSet<String> temp1=MD5Set.subSet(lowerbound, FF);
			
			while(!temp.isEmpty())
			{
				String tempHash=temp.first();
				String tempKey=HashKey.get(tempHash);
				String tempValue=get(tempKey);
				KeyValuePair tempPair=new KeyValuePair(tempKey,tempValue);
				res.add(tempPair);
				MD5Set.remove(tempHash);
				temp.remove(tempHash);
			}
			while(!temp1.isEmpty())
			{
				String tempHash=temp1.first();
				String tempKey=HashKey.get(tempHash);
				String tempValue=get(tempKey);
				KeyValuePair tempPair=new KeyValuePair(tempKey,tempValue);
				res.add(tempPair);
				MD5Set.remove(tempHash);
				temp1.remove(tempHash);
			}
			
		}
		else
		{
			SortedSet<String> temp=MD5Set.subSet(lowerbound, upperbound);
			while(!temp.isEmpty())
			{
				String tempHash=temp.first();
				String tempKey=HashKey.get(tempHash);
				String tempValue=get(tempKey);
				KeyValuePair tempPair=new KeyValuePair(tempKey,tempValue);
				res.add(tempPair);
				MD5Set.remove(tempHash);
			}
		}
		return res;
	}
	
    private boolean get_EI_valid(String file_path, int position) throws IOException
    {
    	int res=0;
    	int read_length=1;
    	
    	
    	String valid="";
    	
    	valid=new String(myutilities.mmap_read(file_path,position,read_length));
    	res=Utilities.decode_128_value_length(valid);
    	if(res==0)
    		return false;
    	else
    		return true;
    }
    
    private void set_EI_invalid(String file_path, int position) throws IOException
    {
    	char invalid=0;
    	String append="";
    	append+=invalid;
    	myutilities.mmap_write(file_path, position, append);
    	
    }
    
    private String get_key(String file_path, int position, int key_length) throws IOException
    {
    	String res="";
    	res=new String(myutilities.mmap_read(file_path, position+6, key_length));
    	return res;
    }
    private int get_EI_key_length(String file_path, int position) throws IOException
    {
    	int res=0;
    	int read_length=1;
    	String value="";
    	int key_offset=1;
    	
    	value=new String(myutilities.mmap_read(file_path,position+key_offset,read_length));
    	
    	res=Utilities.decode_128_value_length(value);
    	
    	return res;
    }
    
    private int get_EI_value_length(String file_path, int position) throws IOException
    {
    	int res=0;
    	int read_length=4;
    	String value="";
    	int value_offset=2;
    	
    	value=new String(myutilities.mmap_read(file_path,position+value_offset,read_length));
    	res=Utilities.decode_128_value_length(value);
    	
    	return res;
    }
    
	private HashMap<String, Integer> lookup_table;	
	private Utilities myutilities;
	private FileSystem myFile;
	private SortedSet<String>MD5Set;
	private HashMap<String,String>HashKey;
	private ReentrantLock WriterLock;
}
