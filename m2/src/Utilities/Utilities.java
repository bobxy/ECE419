package Utilities;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class Utilities {

	public Utilities() {
		// TODO Auto-generated constructor stub
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
	public String cHash(String myStr) throws UnsupportedEncodingException, NoSuchAlgorithmException{
		
		byte[] temp = myStr.getBytes("UTF-8");
		
		MessageDigest md = MessageDigest.getInstance("MD5");
		
		byte [] hash = md.digest(temp);
		
		return byteArrayToHex(hash);
	}
	
	public String byteArrayToHex (byte[] myByteArray){
		
		StringBuilder sb = new StringBuilder(myByteArray.length*2);
		
		for (byte b:myByteArray){
			sb.append(String.format("%02x", b));
		}
		
		return sb.toString();
	}

}