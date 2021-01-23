# Distributed-key-value-store
To get a practical insight into building a distributed key-value store

WHAT IS A KEY-VALUE STORE?
Simply put a store that is capable of storing data indexed by a key
Key is a string of characters
Value is a string of characters
However, the value is a JSON object

ALGORITHM/DESIGN
Multiple servers (here three) run as separate threads. When the
servers start, one of them is elected as the master. The master reads in the
key value pairs of all servers and the corresponding replica server. Each
server gets its znode path when it is initialized. When starting up, the server
gets its port number and filename and the port number and file name of the
server it holds replicated data of, from the znode. Then, it reads in its data
from the file. It also takes in replicated data. It then opens a server socket at
its port and waits for a client to connect.
The client takes in requests from the user. For every request, it first contacts
the master server. The master server goes through its data about all servers
and sends the client the port number of the server it needs to connect to. If
the request is invalid, it is reported to the client. Here, two things may
happen:
1. The server is up and running. The client contacts it, gets the necessary
data and reports it back to the user.
2. The connection to the server fails. The client contacts the master again
and the master finds the address of the replica server. The client gets the
required data from the replica server.
The client stops when the user gives the quit command. The servers stop
when the number 0 is given. When the servers stop, their data is written back
into the files.
