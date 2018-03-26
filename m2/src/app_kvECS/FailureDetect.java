package app_kvECS;

import java.io.UnsupportedEncodingException;
import java.security.NoSuchAlgorithmException;

public class FailureDetect extends Thread{
	ECSClient ecsClient;
	public FailureDetect(ECSClient ecs){
		ecsClient = ecs;
	}
	
	public void run()
	{
		while(ecsClient != null)
		{
			try {
				ecsClient.ExecuteCrash();
				Thread.sleep(30000);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
}
