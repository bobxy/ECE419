package Utilities;

import java.io.IOException;
import java.io.RandomAccessFile;

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
		System.out.print("length is:"+value_length.length());
		for(int i=0;i<value_length.length();i++)
		{
			res=res*128;
			res=res+( value_length.charAt(i)-0);
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
	
	
	
    static final String FILEPATH = "/nfs/ug/homes-0/y/yangxi42/Desktop/ECE419/m1/test/test.file";

    public static void main(String[] args) {


        //try {

            //System.out.println(new String(mmap_read(FILEPATH, 0, 100)));
            //mmap_write(FILEPATH, 0,"tony@!");
           // System.out.println(new String(mmap_read(FILEPATH, 0, 100)));
            //System.out.println(encode_value_length(500));
            //System.out.println("is:"+encode_128_value_length(10));
            System.out.println("is:"+decode_128_value_length(encode_128_value_length(1000))+"haha");
            //System.out.println(encode_128_value_length(1000));
            //System.out.println(encode_256_value_length(10000));
            //System.out.println(encode_256_value_length(10));
            //System.out.println(encode_256_value_length(10));
            
            
        //} //catch (IOException e) {
       //e.printStackTrace();
       //}
 
    }

}
