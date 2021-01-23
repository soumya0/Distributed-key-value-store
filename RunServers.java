import java.io.*;
public class RunServers
{
	public static void main(String[] args)throws IOException
	{
		final Server server1 = new Server("/Node1");		// create ser3ver object 1 handling znode 'Node1'
		final Server server2 = new Server("/Node2");		// create server object 2 handling znode 'Node2'
		final Server server3 = new Server("/Node3");		// create server object 3 handling znode 'Node3'
		
		Thread serverRun1 = new Thread(new Runnable()	// thread for server 1
		{
			public void run()
			{
				try{
					server1.run();					// calling server 1 run() method
					server1.createESNode();		// calling server 1 createESNode() method
				}
				catch(Exception e){}
			}
		});
		Thread serverRun2 = new Thread(new Runnable()	// thread for server 2
		{
			public void run()
			{
				try
				{
					server2.run();					// calling server 2 run() method
					server2.createESNode();		// calling server 2 createESNode() method
				}
				catch(Exception e){}
			}
		});
		Thread serverRun3 = new Thread(new Runnable()	// thread for server 3
		{
			public void run()
			{
				try
				{
					server3.run();					// calling server 3 run() method
					server3.createESNode();		// calling server 3 createESNode() method
				}catch(Exception e){}
			}
		});
		try				//all servers starting together
		{
			serverRun1.start();
			serverRun2.start();
			serverRun3.start();
			serverRun1.join();
			serverRun2.join();
			serverRun3.join();
		}		
		catch(Exception e){}

		Thread serversocket1 = new Thread(new Runnable()		// thread for server 1 socket 
		{
			public void run()
			{
				server1.runSocket();
			}
		});
		
		Thread serversocket2 = new Thread(new Runnable()		// thread for server 2 socket 
		{
			public void run()
			{
				server2.runSocket();
			}
		});
		
		Thread serversocket3 = new Thread(new Runnable()		// thread for server 3 socket 
		{
			public void run()
			{
				server3.runSocket();
			}
		});
		Thread serverStop = new Thread(new Runnable()		// thread for stopping a thread
		{
			public void run()
			{
				try
				{
					BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
					int quit = Integer.parseInt(br.readLine());	// read quit from server's terminal
					while(quit!=0)											
					{
						if(quit==1)														// if quit=1, stop server 1
						{
							server1.running=false;server1.sersock.close();
						}
						if(quit==2)														// if quit=2, stop server 2
						{
							server2.running=false;server2.sersock.close();
						}
						if(quit==3)														// if quit=3, stop server 3
						{
							server3.running=false;server3.sersock.close();
						}
						System.out.println("Stopped Server "+quit);
						quit = Integer.parseInt(br.readLine());
					}
					server1.running=false;		// if quit=0, stop all servers
					server1.sersock.close();
					server2.running=false;
					server2.sersock.close();
					server3.running=false;
					server3.sersock.close();
				}
				catch(IOException e){System.err.println(e);}
			}
		});
		try		//running all simultaneously
		{
			serversocket1.start();
			serversocket2.start();
			serversocket3.start();
			serverStop.start();
			serversocket1.join();
			serversocket2.join();
			serversocket3.join();
			serverStop.join();
		}		
		catch(Exception e){}
	}
		
}
