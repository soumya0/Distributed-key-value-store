import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.AsyncCallback.StatCallback;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;
public class Server
{
	private ZooKeeper zoo;
	private static ZooKeeperConnection conn;
	final CountDownLatch connectedSignal = new CountDownLatch(1);
	HashMap<String, String> memory = new HashMap<String, String>();
	HashMap<String, String> repMemory = new HashMap<String, String>();
	private String path;
	int port;
	String filename;
	String repFilename;
	String servers;
	String keyRange[];
	boolean running=true;
	ServerSocket sersock;

	public static int masterPort;
	public static String[][] masterData;
	public static boolean masterReq=true;
	public static boolean duplicate=false;
	public static boolean writeRep=false;
	public String electionpath="";
	public void declareMaster()throws IOException
	{
		BufferedReader br = new BufferedReader(new FileReader("masterFile.txt"));
		String str = br.readLine();
		String data="";
		while(str != null)
		{
			data += str+":";
			str = br.readLine();
		}
		String []temp = data.split(":");
		masterData = new String[temp.length][];
		for(int i=0;i<temp.length;i++)
		{
			masterData[i] = temp[i].split(",");
		}
	}

	public Server(String path)throws IOException
	{
		this.path=path;
		servers="localhost";
	}
	
	// Create Ephemeral_Sequential znode
	public void createESNode()
	{
		String node="/election/p_";
		byte[] data="".getBytes();
		try
		{
			electionpath = zoo.create(node, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
		} 
		catch (KeeperException | InterruptedException e){}
		attemptForLeaderPosition();
	}
	
	
	private void attemptForLeaderPosition()
	{
		List<String> childNodes = null;
		try
		{
			childNodes = zoo.getChildren("/election", false);
			Collections.sort(childNodes);
			int index = childNodes.indexOf(electionpath.substring(electionpath.lastIndexOf('/') + 1));
			if(index == 0)
			{
				masterPort=port;
				// set data in leader znode
				byte[] data = Integer.toString(port).getBytes();			
				zoo.create("/leader", data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
				try{declareMaster();}catch(IOException e){}
			} 
		}
		catch (KeeperException | InterruptedException e){}
	}	
	public String getDupServer(String portNo)
	{
		int i,j;
		if(this.port==masterPort)
		{
			try
			{
				for(i=0;i<masterData.length;i++)
				{
					if(masterData[i][masterData[i].length-2].equals(portNo))
					{
						return masterData[i][masterData[i].length-1];
					}
				}
			}
			catch(Exception e)
			{
				System.err.println(e);
			}
			return "-1";
		}
		else return "-1";
	}
	public String runMaster(String req)
	{
		int i,j;
		if(this.port==masterPort)
		{
			try
			{
				for(i=0;i<masterData.length;i++)
				{
					for(j=0;j<masterData[i].length-2;j++)
					{
						if(masterData[i][j].equalsIgnoreCase(req.split(" ")[1].substring(0,masterData[i][j].length())))
						{
							return masterData[i][masterData[i].length-2];
						}
					}
				}
			}
			catch(Exception e)
			{
				System.err.println(e);
			}
			return "-1";
		}
		else return "-1";
	}
	public void run() throws Exception
	{
		try
		{
			String data="";
			conn = new ZooKeeperConnection();
			zoo = conn.connect(servers);
			Stat stat =zoo.exists(path, true);
			if(stat != null)
			{
				byte[] b = zoo.getData(path, new Watcher(){
					public void process(WatchedEvent we)
					{
						if (we.getType() == Event.EventType.None)
						{
							switch(we.getState())
							{
								case Expired:
									break;
							}
						}
						else
						{
							try
							{
								byte[] bn = zoo.getData(path,false, null);
								String data = new String(bn,"UTF-8");
							}
							catch(Exception ex)
							{
								System.out.println(ex.getMessage());
							}
						}
					}
				}, null);
				data = new String(b, "UTF-8");
			}
			else
			{
				System.out.println("Node does not exists");
			}

			String[] servs = data.split(":");
			String[] da = servs[0].split(",");
			port = Integer.parseInt(da[0]);
			filename=da[1];
			BufferedReader br = new BufferedReader(new FileReader(filename));
			String str = br.readLine();
			keyRange = str.split(",");
			str = br.readLine();
			while(str != null)
			{
				String vals[] = str.split(",");
				memory.put(vals[0],vals[1]);
				str = br.readLine();
			}
			da = servs[1].split(",");
			repFilename=da[1];
			br = new BufferedReader(new FileReader(repFilename));
			str = br.readLine();
			str = br.readLine();
			while(str != null)
			{
				String vals[] = str.split(",");
				repMemory.put(vals[0],vals[1]);
				str = br.readLine();
			}

			conn.close();
		}
		catch(Exception e)
		{
			conn.close();
			System.err.print(e);
		}
	}
	
	public void runSocket()
	{
		try
		{
			sersock = new ServerSocket(port);
			Socket sock = sersock.accept();
			if(!running)
			{
				System.out.println(port+" stopped running.");
				sock.close();
				sersock.close();
				saveData();
				return;
			}
			OutputStream ostream = sock.getOutputStream();
			PrintWriter pwrite = new PrintWriter(ostream, true);
			InputStream istream = sock.getInputStream();
			BufferedReader receiveRead = new BufferedReader(new InputStreamReader(istream));
			String request, sendMessage;
			while(running)
			{
				request = receiveRead.readLine();
				if(request!=null)
				{
					sendMessage = validRequest(request);
					if(this.port!=masterPort)
					{
						pwrite.println(sendMessage);
						pwrite.flush();
					}
					if(!sendMessage.equals("INVALID REQUEST"))
					{
						if(this.port==masterPort && masterReq)
						{
							String req = runMaster(request);
							if(Integer.parseInt(req)==this.port || Integer.parseInt(req)==-1)
								masterReq=false;
							pwrite.println(req);
							pwrite.flush();
						}
						else
						{
							if(this.port==masterPort && !masterReq)
							{
								pwrite.println(sendMessage);         
								pwrite.flush();
							}
							processRequest(request,pwrite);
							masterReq=true;
						}
					}
					else if(this.port!=masterPort)
					{
						pwrite.println("TRY AGAIN:");
						pwrite.flush();
					}
					else if(!masterReq)
					{
						pwrite.println(sendMessage);
						pwrite.flush();
						pwrite.println("TRY AGAIN:");
						pwrite.flush();
						masterReq=true;
					}
					else
					{
						if(request.equals("ConnectException"))
						{
							String failedPort = receiveRead.readLine();
							duplicate = true;
							String dup = getDupServer(failedPort);
							if(Integer.parseInt(dup)==this.port)
								masterReq = !masterReq;
							pwrite.println(dup);
							pwrite.flush();
						}
						else
						{
							masterReq=!masterReq;
							pwrite.println("-2");
							pwrite.flush();						
						}
					}
				}
				else
				{
					sock.close();
					sock = sersock.accept( );
					ostream = sock.getOutputStream();
					pwrite = new PrintWriter(ostream, true);
					istream = sock.getInputStream();
					receiveRead = new BufferedReader(new InputStreamReader(istream));
				}
				if(!running)		
				{
					System.out.println(port+" stopped running.");
					sersock.close();
				}
			}
			saveData();
			sock.close();
			System.out.println("Socket closed "+port);
		}
		catch(SocketException e)
		{
			if(!running)
			{
				saveData();
			}
		}
		catch(Exception e)
		{
			System.out.println("While runSocket "+this.port);
			System.err.println(e);
		}
		System.out.println("Stopped RUNNING:"+this.port);
	} 	
	public void saveData()
	{
		try
		{
			BufferedWriter bw = new BufferedWriter(new FileWriter(new File(filename)));
			for(int i=0;i<keyRange.length;i++)
			{
				bw.write(keyRange[i]);
				if(i!=keyRange.length-1)
					bw.write(",");
				else
					bw.write("\n");
			}
			Iterator iter = memory.entrySet().iterator();
			while(iter.hasNext())
			{
				Map.Entry k = (Map.Entry)iter.next();
				bw.write(k.getKey()+","+k.getValue()+"\n");
			}
			bw.close();
			if(writeRep)
			{
				BufferedReader br = new BufferedReader(new FileReader(repFilename));
				String st = br.readLine();
				String str = br.readLine();
				while(str != null)
				{
					String vals[] = str.split(",");
					if(repMemory.get(vals[0])==null)
					{
						repMemory.put(vals[0],vals[1]);
					}
					str = br.readLine();
				}
				br.close();
				bw = new BufferedWriter(new FileWriter(new File(repFilename)));
				bw.write(st+"\n");

				iter = repMemory.entrySet().iterator();
				while(iter.hasNext())
				{
					Map.Entry k = (Map.Entry)iter.next();
					bw.write(k.getKey()+","+k.getValue()+"\n");
				}
				bw.close();
			}
		}
		catch(IOException e){}
	}
	public ZooKeeper connect(String host) throws IOException,InterruptedException
	{
		zoo = new ZooKeeper(host,port,new Watcher()
		{
			public void process(WatchedEvent we)
			{
				if (we.getState() == KeeperState.SyncConnected)
				{
					connectedSignal.countDown();
				}
			}
		}
		);
		connectedSignal.await();
		return zoo;
	}
	public void processRequest(String request,PrintWriter pwrite)
	{
		HashMap<String, String> mem = memory;
		if(duplicate)
		{
			mem = repMemory;
			duplicate=false;
			if(!writeRep)
				writeRep=true;
		}
		String part[] = request.split(" ",2);
		String command = part[0];
		if(command.equals("get"))
		{
		     	pwrite.println(mem.get(part[1]));             
		    	pwrite.flush();
		}
		else if(command.equals("put"))
		{
			String vals[] = part[1].split(",");
			mem.put(vals[0],vals[1]);
		     	pwrite.println("PUT COMPLETED");
		    	pwrite.flush();
		}
		else if(command.equals("getmultiple"))
		{
			String vals[] = part[1].split(",");
			String rep="";
			for(String val : vals)
			{
				rep +=val+":"+mem.get(val)+", ";
			}
			rep=rep.substring(0,rep.length()-2);
		     	pwrite.println(rep);
		    	pwrite.flush();
		}
	}
	public String validRequest(String request)
	{
		String part[] = request.split(" ");
		if(part.length!=2)
			return "INVALID REQUEST";
		String command = part[0];
		if(command.equalsIgnoreCase("get"))
		{
			String key=part[1];
			String[] keyar=key.split(",");
			if(keyar.length>1)
				return "INVALID REQUEST";
			return "GET SUCCESSFUL";
		}
	
		else if(command.equalsIgnoreCase("put"))
		{
			String keyvaluecom=part[1];
			String[] keyval=keyvaluecom.split(",");
			if(keyval.length!=2)
				return "INVALID REQUEST";
			String key=keyval[0];
			String value=keyval[1];
			return "PUT SUCCESSFUL";
		}
		else if(command.equalsIgnoreCase("getmultiple"))
		{
			String keyscom=part[1];
			String[] keys=keyscom.split(",");
			return "GETMULTIPLE SUCCESSFUL";
		}		
		return "INVALID REQUEST";					
	}
}
