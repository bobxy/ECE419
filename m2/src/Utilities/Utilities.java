package Utilities;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.Serializable;

import java.io.*;
import java.util.*;

import common.KVMessage.StatusType;

import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;


public class Utilities {
	/**
	 * 
	 */
	public Utilities() {
		// TODO Auto-generated constructor stub
		//Key to MD5 lookup

	}

	
	public  String encode_value_length(int value_length)
	{
		String res="";
		int mask=100000;
		for(int i=0;i<6;i++)
		{
			int append=value_length/mask;
			res=res+append;
			
			value_length=value_length-append*mask;
			mask=mask/10;
		}
		return res;
	}
	
	public static  String encode_128_value_length(int value_length)
	{
		String res="";
		int  mask=0x7f;
		byte[] temp=new byte[4];

		for(int i=3;i>=0;i--)
		{
			temp[i]=(byte) (mask&value_length&0xff);
			value_length=value_length>>>7;
			res=(char)(temp[i]&0xFF)+res;
			
		}

		return res;
	}
	
    
	public  int decode_value_length(String value_length)
	{
		return Integer.parseInt(value_length);
	}
	
	public static  int decode_128_value_length(String value_length)
	{
		int res=0;
		for(int i=0;i<value_length.length();i++)
		{
			res=res*128;
			res=res+( value_length.charAt(i));
		}
		
		return res;
	}
	
	public  byte[] mmap_read(String file_path,int start_position,int read_length) throws IOException
	{

        RandomAccessFile file = new RandomAccessFile(file_path, "r");

        file.seek(start_position);

        byte[] bytes = new byte[read_length];

        file.read(bytes);

        file.close();

        return bytes;

	}
	
	public  void mmap_write(String file_path,int start_position,String data) throws IOException
	{
        RandomAccessFile file = new RandomAccessFile(file_path, "rw");
        
        file.seek(start_position);
        
        file.write(data.getBytes());
        
        file.close();
	}
	
	public boolean InvaildKey(String key)
	{
		return (key == null || key.contains(" ") || key.isEmpty() || key.length() > 20);
	}
	
	//consistent hashing; return hex string
	public String cHash(String key) throws UnsupportedEncodingException, NoSuchAlgorithmException{
		
		byte[] temp = key.getBytes("UTF-8");
		
		MessageDigest md = MessageDigest.getInstance("MD5");
		
		byte [] hash = md.digest(temp);
		
		StringBuilder sb = new StringBuilder(hash.length*2);
		
		for (byte b:hash){
			sb.append(String.format("%02x", b));
		}
		
		return sb.toString();

	}

	public byte[] SerializeHashMapToByteArray(HashMap<String,String> mymap) throws Exception
	{
		ByteArrayOutputStream bos = new ByteArrayOutputStream();
		ObjectOutputStream out = new ObjectOutputStream(bos);
		out.writeObject(mymap);
		out.flush();
		return bos.toByteArray();
	}
	
	public HashMap<String,String> DeserializeByteArrayToHashMap(byte[] mymap) throws Exception
	{
		ByteArrayInputStream bis = new ByteArrayInputStream(mymap);
		ObjectInputStream in = new ObjectInputStream(bis);
		return (HashMap<String,String>)in.readObject();
	}

	public String StatusCodeToString(StatusType status)
	{
		return Integer.toString(status.ordinal());
	}
	
	public String FindNextOne(HashMap<String,String> mymap,String hashname)
	{
		String res="";
		Set<String> mykeys=mymap.keySet();
		List<String> SortedList = new ArrayList<String>(mykeys);
		Collections.sort(SortedList);
		
		if(SortedList.get(SortedList.size()-1).equals(hashname))
		{
			res=SortedList.get(0);
		}
		else
		{
			for(int i=0;i<SortedList.size();i++)
			{
				if(SortedList.get(i).equals(hashname))
				{
					res=SortedList.get(i+1);
					break;
				}
			}
		}

		return res;
	}
	
	public String FindBeforeOne(HashMap<String,String> mymap,String hashname)
	{
		String res="";
		Set<String> mykeys=mymap.keySet();
		List<String> SortedList = new ArrayList<String>(mykeys);
		Collections.sort(SortedList);
		
		if(SortedList.get(0).equals(hashname))
		{
			res=SortedList.get(SortedList.size()-1);
		}
		else
		{
			for(int i=1;i<SortedList.size();i++)
			{
				if(SortedList.get(i).equals(hashname))
				{
					res=SortedList.get(i-1);
					break;
				}
			}
		}

		return res;
	}
	
	public boolean IsResponsible(String HashedKey, String currentLower, String currentUpper)
	{
		if(currentLower.compareTo(currentUpper)>0)
		{
			String zero="";
			String FF="";
			for(int i=0;i<32;i++)
			{
				zero+="0";
				FF+="f";
			}
			if(HashedKey.compareTo(currentLower)>0 && FF.compareTo(HashedKey)>0)
				return true;
			else if(HashedKey.compareTo(zero)>0 && currentUpper.compareTo(HashedKey)>0)
				return true;		
		}
		else
		{
			if(HashedKey.compareTo(currentLower)>0 && currentUpper.compareTo(HashedKey)>0)
				return true;
		}
		return false;
	}
	
	public enum servStatus {
		added,
		adding,
		removed,
		removing,
		started,
		starting,
		stopped,
		stopping,
		sending,
		adding_starting,
		exiting,
		exited,
		none
	}
	public enum servStrategy {
        none,
        LRU,
        LFU,
        FIFO
    };

}