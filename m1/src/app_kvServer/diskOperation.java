package app_kvServer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

import Utilities.Utilities;

public class diskOperation {

	public diskOperation() {
		// TODO Auto-generated constructor stub
		//populate lookup
		myutilities=new Utilities();
		lookup_table = new HashMap<String, Integer>();
		myFile = new FileSystem();
		myFile.insert_file("test.file");
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
	
	public void load_lookup_table() throws IOException
	{
		ArrayList<String> myfiles=myFile.get_all_file_path();
		System.out.print("goes to here\n");
		for(int i=0;i<myfiles.size();i++)
		{
			System.out.print("goes to here2\n");
			int position=0;
			String file_path=myfiles.get(i);
			int total_length=0;
			while(position<myFile.get_file_size(file_path))
			{
				System.out.print("my position is"+position+"\n");
				boolean isvalid=get_EI_valid(file_path,position);
				if(isvalid)
				{
					System.out.print("goes to here3\n");
					//if valid, update lookup table
					int key_length=get_EI_key_length(file_path,position);
					int value_length=get_EI_value_length(file_path,position);
					total_length=0;
					total_length=6+key_length+value_length;
					String key=get_key(file_path,position,key_length);
					lookup_table.put(key, position);
					
					
				}
				position=1+total_length;
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
    
	public void put(String key, String value) throws IOException{
		String file_path=myFile.get_file_path(key);
		int file_size=myFile.get_file_size(file_path);
		//append to the end of file
		int new_file_size=0;
		String ei="";
		if(lookup_table.containsKey(key))
		{
			//need to set invalid for old key value
			System.out.println("set invalid");
			int position=lookup_table.get(key);
			set_EI_invalid(file_path,position);
		}
		
		//create ei;
		ei=ei+1; //valid =1;
		ei=ei+(char)key.length();
		ei=ei+Utilities.encode_128_value_length(value.length());
		String res=ei+key+value;
		int position=file_size+1;
		//write to file
		myutilities.mmap_write(file_path, position, res);
		//update lookup table
		lookup_table.put(key, position);
		
		
		new_file_size=file_size+res.length();
		myFile.update_file_size(file_path, new_file_size);
		
		return;
			
	}

	public void clearStorage(){
		//clean up lookup table
		lookup_table.clear();
		//clean up file system
		myFile.clearFile();
		
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
    	String invalid="0";
    	myutilities.mmap_write(file_path, position, invalid);
    	
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
    
    
    private void remove_disk(String file_path,String key) throws IOException
    {
    	if(!lookup_table.containsKey(key)||lookup_table.get(key)==-1)
    	{
    		//doesn't have entry or already removed
    		return;
    	}
    	{
    		int position=lookup_table.get(key);
    		set_EI_invalid(file_path, position);
    		lookup_table.put(key, -1);
    	}
    }


	
	private HashMap<String, Integer> lookup_table;	
	private Utilities myutilities;
	private FileSystem myFile;

}
