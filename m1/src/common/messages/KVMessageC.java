package common.messages;

public class KVMessageC implements KVMessage{
	
	private String key;
	private String value;
	private StatusType status;

	public KVMessageC() {
		key = null;
		value = null;
		status = null;
	}

	public String getKey(){
		return key;
	}
	
	public String getValue(){
		return value;
	}
	
	
	public StatusType getStatus(){
		return status;
	}
	
	public void StrToKVM (String str){
		// TODO Auto-generated constructor stub
		String[] splited = str.split(" ");
		status = StatusType.values()[Integer.parseInt(splited[0])];
		key = splited[1];
		int nKeyLength = key.length();
		if(3 + nKeyLength >= str.length())
			value = "";
		else
			value = str.substring(3 + nKeyLength);
	}

}
