package app_kvServer;

import java.io.File;
import java.io.IOException;
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
	
	public void insert_file(String file_path)
	{
		
		File file =new File(file_path);
		if(!file.exists())
			try {
				file.createNewFile();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		file_num_path.add(file.getAbsolutePath());
		file_file_size.put(file.getAbsolutePath(), (int) file.length());
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

			File file = new File(file_num_path.get(i)).getAbsoluteFile();

			if(file.exists())
			{
				file.delete();
				
			}
			if(!file.exists())
				try {
					file.createNewFile();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
		}
	}
	private HashMap<String, Integer> file_file_size;

	//index to path
	private ArrayList<String> file_num_path;

}
