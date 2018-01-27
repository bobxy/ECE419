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
	
	public KVMessageC(String sKey, String sValue, StatusType statusType) {
		key = sKey;
		value = sValue;
		status = statusType;
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
		String[] splited = str.split(" ");
		status = StatusType.values()[Integer.parseInt(splited[0])];
		key = splited[1];
		int nKeyLength = key.length();
		if(3 + nKeyLength >= str.length())														//e.g 0 key value 
			value = "";
		else
			value = str.substring(3 + nKeyLength);
	}

}
