package app_kvServer;

public class KeyValuePair {
	private String key;
	private String value;
	public KeyValuePair() {
		// TODO Auto-generated constructor stub
	}
	public KeyValuePair(String tempkey,String tempvalue)
	{
		key=tempkey;
		value=tempvalue;
	}
	
	public String getKey()
	{
		return key;
	}
	public String getValue()
	{
		return value;
	}
}
