package app_kvServer;

import java.io.IOException;
import java.util.HashMap;

import Utilities.Utilities;

public class diskOperation {

	public diskOperation() {
		// TODO Auto-generated constructor stub
	//populate lookup
		myutilities=new Utilities();
		lookup_table = new HashMap<String, Integer>();	
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
	
	
    public String get_disk(String file_path,String key) throws IOException{
        	String res="";
        	int position=0;
        	
        	if(!lookup_table.containsKey(key))
        	{
        		return res;
        	}
        	else
        	{
        		
        		position=lookup_table.get(key);
        		if(!get_EI_valid(file_path,position))
        		{
        			//check if the entry is valid
        			return res;
        		}
        		
        		int read_length=get_EI_value_length(file_path,position);
        		int offset=6;
        		res=new String(myutilities.mmap_read(file_path,position+offset,read_length));
        	}
        	
        	return res;
	}

    public boolean get_EI_valid(String file_path, int position) throws IOException
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
    
    public void set_EI_invalid(String file_path, int position) throws IOException
    {
    	String invalid="0";
    	myutilities.mmap_write(file_path, position, invalid);
    	
    }
    
    public int get_EI_value_length(String file_path, int position) throws IOException
    {
    	int res=0;
    	int read_length=4;
    	String value="";
    	int offset=1;
    	
    	value=new String(myutilities.mmap_read(file_path,position+offset,read_length));
    	
    	res=Utilities.decode_128_value_length(value);
    	
    	return res;
    }
    
    
    public void remove_disk(String file_path,String key) throws IOException
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

	public int put_disk(String file_path, int file_size, String key, String value) throws IOException{
		//append to the end of file
		int new_file_size=0;
		String ei="";
		if(lookup_table.containsKey(key))
		{
			//need to set invalid for old key value
			int position=lookup_table.get(key);
			set_EI_invalid(file_path,position);
		}
		
		//create ei;
		ei=ei+1; //valid =1;
		ei=ei+(char)key.length();
		ei=ei+Utilities.encode_128_value_length(value.length());
		String res=ei+key+value;
	
		int position=file_size+1;
		lookup_table.put(key, position);
		myutilities.mmap_write(file_path, position, res);
		
		new_file_size=file_size+res.length();
		return new_file_size;
			
	}
	
	//iterate through files tablescan
	public void load_index_storage()
	{
		
	}
	
	//clean garbage data file
	
	//

	
	private HashMap<String, Integer> lookup_table;	
	private Utilities myutilities;

}
