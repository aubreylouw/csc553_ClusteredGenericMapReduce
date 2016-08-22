Hi Professor -

This assignment was a lot of fun! Below is a brief description of the implementation architecture and instructions for running the code.

Implementation Architecture
===========================
The implementation follows the basic client-server model where ClusteredGenericMapReduce functions as the server and MapReduceClient the client. 
The server is a cluster consisting of a minimal set of 1 receptionist, 1 service, and 1 worker. Unless specific runtime arguments are provided, the 
cluster will consist of 2 worker nodes.

The receptionist is responsible for communicating with external clients. It receives job requests, responds with a job ID, and submits the job to the
map-reduce service for processing. Finally, the receptionist responds to the client with a job success or failure message.

The map-reduce service is responsible for coordinating the map and reduce tasks for each job. It delagates both the map and reduce operations to dedicated 
consistent hashing router pools. These routers deploy the mapping and reducing actors on the worker nodes currrently running in the cluster. The service is
responsible for sending the "flush" messages to all mappers/reducers and therefore must keep tabs on the current state of each job. This latter state consists of
the number of mapping tasks outstanding and the number of reducers engaged. In the event of an actor failure running on a worker node, the service terminates
the job and sends a job failure message to the receptionist; otherwise, once the last reducer sends back results, the service locally aggregates all of the 
results received thus far and sends that result set back to the receptionist.

The worker nodes are "dumb actors" that do nothing except host mapper or reducer actors deployed by the map-reduce service. The mapper/reducer actors
are generic in the sense that they can execute arbitrary client code associated with a job ID. The intent is that one actor can handle job A, job B, job C,
etc serially even if A, B, and C have very different source data, data fetch operations, map operations, or reduce operations.

The mapper actor running on the worker node executes client map operations and data fetch operations. The latter is delagated to a child actor so that IO 
failures can be handled by the mapper's supervisor strategy. In the event of a failure, the mapper sends a map failed message to the map-reduce service who 
then cancels the job and notifies the client. The mapper sends intermediate results to the map-reduce service which in turn just sends a hash envelope to the 
reducer router.

The reducer actor running on the worker node executes client reduce operations. Results are sent back to the map-reduce service once all map operations are 
complete (the map-reduce service broadcasts this message by jobID).

There can be 1...N concurrent clients connecting to the map-reduce server. The clients send job requests to the server, receive back a job acknowledgement,
and then await either a failure/success message. 

Note that two packages, messages (all shared messages between nodes in the cluster and between the cluster and the clients) and implementations (client
implementations of the map, reduce, and data fetch interfaces), are shared between the server and client codebases.

Execution Instructions
======================
(1) Start the map-reduce cluster
	- in the ..\ClusteredGenericMapReduce folder, execute command 'sbt clean compile "run x"' where x indicates the number of worker nodes desired
(2) Start as many map-reduce clients as desired
	- in the \ClusteredGenericMapReduceClient folder, exeute command 'sbt clean compile "run x y"' 
		where x is 1, 2, or 3 depending on the type of problem
		- 1 = word count
		- 2 = reverse index
		- 3 = first step in page rank
		where y is the a directory path where the test files are stored [see resource_files directory in the submission]
	- NOTE that the app is hard-coded with certain test files but this is just an implementation detail