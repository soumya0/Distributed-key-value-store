import java.io.*;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;
import java.io.IOException;

public class SetZoo
{
	private static ZooKeeper zk;
	private static ZooKeeperConnection conn;
	public static void main(String[] args) throws InterruptedException, KeeperException, IOException
	{
		byte[] data;
		try
		{
			conn = new ZooKeeperConnection();
			zk = conn.connect("localhost");
			data = "3000,input1.txt:3001,input2.txt:3002,input3.txt".getBytes();
			zk.create("/Node1", data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);	 // create persistent znode 'Node1'
			data = "3001,input2.txt:3002,input3.txt:3000,input1.txt".getBytes();
			zk.create("/Node2", data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);	 // create persistent znode 'Node2'
			data = "3002,input3.txt:3000,input1.txt:3001,input2.txt".getBytes();
			zk.create("/Node3", data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);	 // create persistent znode 'Node3'
			data = "".getBytes();
			zk.create("/election", data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT); // create persistent znode 'election'
		}
		catch(Exception e)
		{
			System.out.println(e.getMessage());
		}
	}
}
