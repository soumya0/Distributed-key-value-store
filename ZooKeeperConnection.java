import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.AsyncCallback.StatCallback;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.data.Stat;

public class ZooKeeperConnection
{
	private ZooKeeper zoo;		// declare zookeeper instance to access ZooKeeper ensemble
	final CountDownLatch connectedSignal = new CountDownLatch(1);
	public ZooKeeper connect(String host) throws IOException,InterruptedException 	// Method to connect zookeeper ensemble.
	{
		zoo = new ZooKeeper(host,7000,new Watcher(){
			public void process(WatchedEvent we) 
			{
				if (we.getState() == KeeperState.SyncConnected)
				{
					connectedSignal.countDown();
				}
			}
		});
		connectedSignal.await();
		return zoo;
	}

	public void close() throws InterruptedException		// close zookeeper connection
	{
		zoo.close();
	}
	public Stat znode_exists(String path) throws KeeperException,InterruptedException	//	method to cheeck if znode exists
	{
		return zoo.exists(path, true);
	}

}
