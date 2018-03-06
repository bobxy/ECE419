package app_kvECS;

//import java classes
import java.io.IOException;
import java.util.concurrent.CountDownLatch;

//import zookeeper classes
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.AsyncCallback.StatCallback;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.data.Stat;

public class ZKConnection {
	
	// declare zookeeper instance to access ZooKeeper ensemble
	private ZooKeeper zk;
	final CountDownLatch connectedSignal = new CountDownLatch(1);

	public ZKConnection() {
		// TODO Auto-generated constructor stub
	}

	// Method to connect zookeeper ensemble.
	public ZooKeeper connect(String host) throws IOException,InterruptedException {
		
	   zk = new ZooKeeper(host,5000,new Watcher() {
			
	      public void process(WatchedEvent we) {

	         if (we.getState() == KeeperState.SyncConnected) {
	            connectedSignal.countDown();
	         }
	      }
	   });
			
	   connectedSignal.await();
	   return zk;
	}

	// Method to disconnect from zookeeper server
	public void close() throws InterruptedException {
	   zk.close();
	}
	
	

}
