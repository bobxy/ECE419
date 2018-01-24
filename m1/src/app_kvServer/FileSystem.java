package app_kvServer;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;

public class FileSystem {

	public FileSystem() {
		// TODO Auto-generated constructor stub
		file_num_path = new ArrayList<String>();
		file_file_size = new HashMap<String, Integer>();
	}
	
	public ArrayList<String> get_all_file_path()
	{
		return file_num_path;
	}
	public int get_file_size(String file_path)
	{
		int res=file_file_size.get(file_path);
		return res;
	}
	public void update_file_size(String file_path, Integer new_file_size)
	{
		file_file_size.put(file_path, new_file_size);
	}
	public void insert_new_file(String file_path)
	{
		file_num_path.add(file_path);
		file_file_size.put(file_path, 0);
	}
	
	public String get_file_path(String key)
	{
		String res=file_num_path.get(get_file_number(key));
		return res;
	}
	//use an array and hash map to store file to file size info. array for random
	
	
	public int get_file_number(String key)
	{
		int hash = 7;
		for (int i = 0; i < key.length(); i++) {
		    hash = hash*31 + key.charAt(i);
		}
		return hash%file_num_path.size();
	}
	
	public void clearFile()
	{
		file_file_size.clear();
		for(int i=0;i<file_num_path.size();i++)
		{
			File file = new File(file_num_path.get(i));
	         
	        if(file.delete())
	        {
	            System.out.println("File deleted successfully");
	        }
	        else
	        {
	            System.out.println("Failed to delete the file");
	        }
		}
		file_num_path.clear();
	}
	private HashMap<String, Integer> file_file_size;

	//index to path
	private ArrayList<String> file_num_path;

}