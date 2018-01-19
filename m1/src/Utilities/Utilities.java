package Utilities;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

public class Utilities {

	public Utilities() {
		// TODO Auto-generated constructor stub
	}

	public static String encode_value_length(int value_length)
	{
		String res="";
		int current_value=value_length;
		while(current_value!=0)
		{
			int temp=current_value%256;
			current_value=current_value/256;
			res=temp+res;
		}
		return res;
	}
	
	public static int decode_value_length(String value_length)
	{
		int res=0;
		int size=value_length.length();
		if(size==0)
			return res;
		for(int i=0;i<size-1;i++)
		{
				
			res+=(value_length.charAt(i)-'0')*256;
		}
		res+=(value_length.charAt(size-1)-'0');
		return res;
	}
	
	public static byte[] mmap_read(String file_path,int start_position,int read_length) throws IOException
	{

        RandomAccessFile file = new RandomAccessFile(file_path, "r");

        file.seek(start_position);

        byte[] bytes = new byte[read_length];

        file.read(bytes);

        file.close();

        return bytes;

	}
	
	public static void mmap_write(String file_path,int start_position,String data) throws IOException
	{
        RandomAccessFile file = new RandomAccessFile(file_path, "rw");
        
        file.seek(start_position);
        
        file.write(data.getBytes());
        
        file.close();
	}
	
	
	
    static final String FILEPATH = "/nfs/ug/homes-0/y/yangxi42/Desktop/ECE419/m1/test/test.file";

    public static void main(String[] args) {


        try {

            System.out.println(new String(mmap_read(FILEPATH, 0, 100)));
            mmap_write(FILEPATH, 0,"tony@!");
            System.out.println(new String(mmap_read(FILEPATH, 0, 100)));
            System.out.println(encode_value_length(256*2));
            System.out.println(decode_value_length("20")+"");
        } catch (IOException e) {
       e.printStackTrace();
       }
 
    }

}
