import java.io.*;                    
import java.net.*;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.data.Stat;

public class Client
{
	private static ZooKeeper zk;
	private static ZooKeeperConnection conn;
	public static void main(String[] args) throws Exception
	{
		{
			conn = new ZooKeeperConnection();
			zk = conn.connect("localhost");
			String data="0";
			Stat stat =zk.exists("/leader", true);			// check if znode 'leader' exists 
			if(stat != null)			// if znode exists
			{
				byte[] b = zk.getData("/leader", new Watcher(){	// get data,i.e., master port from the znode
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
								byte[] bn = zk.getData("/leader",false, null);
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
			else		// znode does not exist
			{
				System.out.println("Node does not exists");
			}
			int masterPort=Integer.parseInt(data);			// set masterPort
			if(masterPort==0)			// if no master
			{
				System.out.println("No Master");
				System.exit(0);
			}
			String receiveMessage, request="";
		  	while(true)			// client handling requests
			{
				BufferedReader keyRead,receiveRead;
				Socket sock;
				OutputStream ostream;
				InputStream istream;
				PrintWriter pwrite;
				int port = masterPort;
				try
				{
					System.out.println("\nEnter a request: ");
					keyRead = new BufferedReader(new InputStreamReader(System.in));				  	// reading from keyboard (keyRead object)
					sock = new Socket("127.0.0.1", masterPort);
					ostream = sock.getOutputStream(); 					// sending to client (pwrite object)
					pwrite = new PrintWriter(ostream, true);				// sending to client (pwrite object)
					istream = sock.getInputStream();
					receiveRead = new BufferedReader(new InputStreamReader(istream));

					request = keyRead.readLine();  // keyboard reading
					if(request.equals("quit"))
						break;
					
					pwrite.println(request);       // sending to server 
					pwrite.flush();                    // flush the data
			     	receiveMessage = receiveRead.readLine();//receive from server
			     	if(receiveMessage != null)
					{
						port = Integer.parseInt(receiveMessage);
			     	}
					System.out.println("Received port:"+port);
					if(port==-1)//unknown keys are handled by master
					{
						System.out.println("Master Lookup Failed");
						port=masterPort;
					}
					else if(port==-2)		//if invalid, master cannot determine which port client must go to
					{
						port=masterPort;		//response to invalid handled by itself
					}
					if(masterPort!=port)
					{
						sock.close();
						sock = new Socket("127.0.0.1", port);						// sending to client (pwrite object)
						ostream = sock.getOutputStream(); 
						pwrite = new PrintWriter(ostream, true);						// receiving from server ( receiveRead  object)
						istream = sock.getInputStream();
						receiveRead = new BufferedReader(new InputStreamReader(istream));
					}
					pwrite.println(request);       // sending to server 
					pwrite.flush();                    // flush the data
			     	receiveMessage = receiveRead.readLine();//receive from server
			     	if(receiveMessage != null)
					{
						System.out.println(receiveMessage); // displaying at terminal
			     	}
					else{throw new ConnectException();}      
			     	receiveMessage = receiveRead.readLine();//receive from server
			     	if(receiveMessage != null)
					{
						System.out.println(receiveMessage); // displaying at terminal
					}
					else{throw new ConnectException();}
				
					sock.close();
					ostream.close();
					pwrite.close();
					istream.close();
					receiveRead.close();  

				}
				catch(ConnectException e)			//The received server did not connect
				{
					try
					{
						sock = new Socket("127.0.0.1", masterPort);						// sending to client (pwrite object)
						ostream = sock.getOutputStream(); 
						pwrite = new PrintWriter(ostream, true);						// receiving from server ( receiveRead  object)
						istream = sock.getInputStream();
						receiveRead = new BufferedReader(new InputStreamReader(istream));
						pwrite.println("ConnectException");
						pwrite.flush();
						pwrite.println(""+port);
						pwrite.flush();
						receiveMessage = receiveRead.readLine();	//receive from server
				     	if(receiveMessage != null)
						{
							System.out.println("Received Duplicate Server Port:"+receiveMessage); // displaying at terminal
							port=Integer.parseInt(receiveMessage);
				     	}
						if(masterPort!=port)
						{
							sock.close();
							sock = new Socket("127.0.0.1", port);							// sending to client (pwrite object)
							ostream = sock.getOutputStream();
							pwrite = new PrintWriter(ostream, true);							// receiving from server ( receiveRead  object)
							istream = sock.getInputStream();
							receiveRead = new BufferedReader(new InputStreamReader(istream));
						}
						pwrite.println(request);       // sending to server 
						pwrite.flush();                    // flush the data
				     	receiveMessage = receiveRead.readLine();//receive from server
				     	if(receiveMessage != null)
						{
							System.out.println(receiveMessage); // displaying at terminal
				     	}         
				     	receiveMessage = receiveRead.readLine();//receive from server
				     	if(receiveMessage != null)
						{
							System.out.println(receiveMessage); // displaying at terminal
				     	}
						sock.close();
					}
					catch(ConnectException ex)//Replica is also down.
					{
						System.out.println("Multiple Servers Down");
						System.exit(0);
					}
					continue;					
				}
				catch(Exception e)
				{
					System.err.println(e);break;
				}
			}	  	
	  	}
	}
	public static Stat znode_exists(String path) throws KeeperException,InterruptedException
	{
		return zk.exists(path, true);
	}
}                        
