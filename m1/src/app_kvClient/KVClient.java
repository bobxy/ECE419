package app_kvClient;

import client.KVCommInterface;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.HashSet;
import java.util.Set;
import logger.LogSetup;

public class KVClient implements IKVClient {
    @Override
    public void newConnection(String hostname, int port) throws Exception{
        // TODO Auto-generated method stub
    	return;
    }

    @Override
    public KVCommInterface getStore(){
        // TODO Auto-generated method stub
        return null;
    }
}
