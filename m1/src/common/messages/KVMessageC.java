package common.messages;

public class KVMessageC implements KVMessage{
	
	private String key;
	private String value;
	private StatusType status;

	public KVMessageC(String str) {
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
		String[] splited = str.split("\\s+");
		key = splited[0];
		value = splited[1];
		status = StatusType.values()[Integer.parseInt(splited[2])];
	}

}
