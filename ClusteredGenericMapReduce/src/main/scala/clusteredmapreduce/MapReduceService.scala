package clusteredmapreduce

import akka.cluster.ClusterEvent.{InitialStateAsEvents, MemberEvent, MemberUp, UnreachableMember}
import akka.actor.{Actor, ActorLogging, ActorPath, ActorRef, Address, Props, RootActorPath}
import akka.cluster.routing.{ClusterRouterPool, ClusterRouterPoolSettings}
import akka.routing.ConsistentHashingRouter.ConsistentHashableEnvelope
import akka.routing.{Broadcast, ConsistentHashingPool}
import scala.collection.immutable.HashMap
import akka.cluster.Cluster
import messages._

class MapReduceService extends Actor with ActorLogging {

  // ********** cluster state finals
  val cluster = Cluster(context.system)
  // the receptionist in contact with the client that ultimately requested a job
  var receptionistSystem:Option[Address] = None

  // ********** mapping state vars
  // {jobID, # of map operations associated} determined by linking the size of the
  // resourceSeq in the initial map reduce job message to that jobID
  // note that since multiple map operations may end up at the same mapper, this is not a count of unique mapping actors
  var mapTasksRemaining: HashMap[Int, Int] = HashMap.empty
  val mapperRouter:ActorRef = context.actorOf(
    ClusterRouterPool(ConsistentHashingPool(0), ClusterRouterPoolSettings(
      totalInstances = 100, maxInstancesPerNode = 4,
      allowLocalRoutees = false, useRole = Some("worker"))).props(Props[Mapper]),
    name = "mapperRouter")

  // ********** mapping state vars
  // {jobID, set of reducer actors} determined by registering an ACK from each reducer that received a reducing op
  // note that one reducer may get all the reducer jobs; this is OK, since a reducer may only send its final results
  // once the map-service has indicated that all mapping ops are done
  var reduceTasksRemaining: HashMap[Int, Set[ActorRef]] = HashMap.empty
  // {jobID, List of reducer results} determined by a cons to the current intermediate result set for the job of each
  // reduce operation emit
  var reduceTaskResults: HashMap[Int, List[String]] = HashMap.empty
  val reducerRouter:ActorRef = context.actorOf(
    ClusterRouterPool(ConsistentHashingPool(0), ClusterRouterPoolSettings(
      totalInstances = 20, maxInstancesPerNode = 2,
      allowLocalRoutees = false, useRole = Some("worker"))).props(Props[Reducer]),
    name = "reducerRouter")

  // subscribe to cluster changes, re-subscribe when restart
  override def preStart(): Unit = {
    cluster.subscribe(self, initialStateMode = InitialStateAsEvents,
      classOf[MemberEvent], classOf[UnreachableMember])
  }

  override def receive: Receive = {

    /*
    The map-reduce service needs a current reference to the receptionist cluster system for sending back
    job statuses (complete, failure, etc.).
     */
    case MemberUp(m) => {
      //log.info(self.path.name + " knows about " + m.address)
      if (m.hasRole("receptionist")) receptionistSystem = Some(m.address)
    }

    /*
    The results from each reducer is locally aggregated.
    The map-reduce service removes the sending reducer from the set of known reducers for this job, and
    sends the final aggregated results back to itself if the set of known reducers is now empty.
     */
    case ReduceOperationEmit(jobID, result) => {

      // aggregate the results from all reducers for the job
      if (!reduceTaskResults.get(jobID).isDefined)
        reduceTaskResults.+=((jobID, List.empty))
      var results = reduceTaskResults.get(jobID).get
      val resultSize = results.size
      val oldSize = results.size
      results = results.:::(result)
      reduceTaskResults.-(jobID)
      reduceTaskResults.+=((jobID, results))
      val newSize = reduceTaskResults.get(jobID).get.size

      log.info("[" + akka.serialization.Serialization.serializedActorPath(self) +
        s"] received ReduceOpEmit# $jobID old rec count: $oldSize message rec count: $resultSize  + -> new rec count $newSize from " +
        sender().path.toStringWithoutAddress + " reducers outstanding? " + reduceTasksRemaining.get(jobID).get.size)

      // determine if all reducers are complete
      if (!reduceTasksRemaining.get(jobID).isDefined) throw new IllegalStateException("unknown reducer")
      var reducersSet: Set[ActorRef] = reduceTasksRemaining.get(jobID).get
      reducersSet -= sender()
      reduceTasksRemaining.+=((jobID, reducersSet))
      if (reducersSet.isEmpty) {
        log.info("[" + akka.serialization.Serialization.serializedActorPath(self) + "] detected job is DONE. Last sender " +
          sender().path.toStringWithoutAddress)
        self ! MapReduceJobComplete(jobID, reduceTaskResults.get(jobID).get)
      }
    }

    /*
    The map-reduce service looks up the current receptionist, sends the job results, and deletes all temporary
    state associated with the jobID
    */
    case MapReduceJobComplete(jobID, results) => {

      //send results back to the receptionist
      val serviceSystemPath: ActorPath = RootActorPath(receptionistSystem.get) / "user" / "receptionist"
      context.system.actorSelection(serviceSystemPath) ! MapReduceJobComplete(jobID, results)

      //do some cleanup to permit GC
      deleteJobDetails(jobID)
    }

    /*
    If any map-operation fails, the receptionist is informed of the failure.
    TODO: The map-reduce service cleans up intermediate state for this jobID
    */
    case MapOperationFailure(jobID, reason) => {
      //send results back to the receptionist
      val serviceSystemPath: ActorPath = RootActorPath(receptionistSystem.get) / "user" / "receptionist"
      context.system.actorSelection(serviceSystemPath) ! MapReduceJobFailure(jobID, reason)
    }
    /*
    The map-reduce service decrements the number of outstanding mapping operations as each mapper sends back it's results.
    If all mapping operations have completed, the service notifies all reducers that no new mappers will be sending results
    for this particular job
    */
    case MapOperationComplete(jobID) => {

      val tasksLeft = mapTasksRemaining.get(jobID).get - 1
      log.info("[" + akka.serialization.Serialization.serializedActorPath(self) + "] received MapOperationComplete#" + jobID +
        " from " + sender().path.toStringWithoutAddress + " tasks left=" + tasksLeft)
      mapTasksRemaining = mapTasksRemaining.+((jobID, tasksLeft))

      if (tasksLeft == 0) reducerRouter ! Broadcast(MapOperationComplete(jobID))
    }

    /*
    The map actor cannot send messages to the reduce router directly since message ordering only has
    a happens before from one sender to another. It is important that reducers never get a map op complete message
    before they get a map operation emit.
    */
    case MapOperationEmit(jobID, key, emit) => {
      reducerRouter ! ConsistentHashableEnvelope(hashKey = key, message = MapOperationEmit(jobID, key, emit))
    }

    /*
    The map-reduce service does the following upon receipt of a new job:
    -- notifies all reducers what reducer function factory to use with this jobID
    -- sends a map operation for each resource in the resourceSeq
    -- records the number of map operations initiated [necessary for determining if the map phase is complete]
    */
    case MapReduceJob(jobID, resourceSeq, dataFetchFuncFactory, mapFuncFactory, reduceFuncFactory) => {

      //log.info("[" + akka.serialization.Serialization.serializedActorPath(self) + "] received MapReduceJob#" + jobID + " for " + resourceSeq.size + " docs")

      // prime all reducers for this job - each recipient will acknowledge the message
      reducerRouter ! Broadcast(ReduceOperation(jobID, reduceFuncFactory))

      // distribute the map operations for each resource according to its consistent hash
      resourceSeq foreach (resource => {
        val dataFetchOp: DataFetchFunction = dataFetchFuncFactory.getNew()
        val mapOp: MapFunction = mapFuncFactory.getNew()

        mapperRouter ! ConsistentHashableEnvelope(hashKey = resource.toString,
          message = (MapOperation(jobID, resource, dataFetchOp, mapOp)))

        //log.info("[" + akka.serialization.Serialization.serializedActorPath(self) + "] sent MapOp#" + jobID + " doc#" + resourceSeq.size)
      })
      mapTasksRemaining = mapTasksRemaining.+((jobID, resourceSeq.size))
    }

    /*
    Each reducer sent a reduce operation for a jobID acknowledges receipt. This allows the map-reduce service
    to track the number of active reducers for a job and thereby to determine how many reduction operations are
    outstanding
    */
    case ReduceOperationAck(jobID, reduceActorRef) => {
      var reducersForThisJob: Set[ActorRef] = Set.empty

      if (reduceTasksRemaining.get(jobID).isDefined)
        reducersForThisJob = reduceTasksRemaining.get(jobID).get

      reducersForThisJob.+=(reduceActorRef)

      reduceTasksRemaining.+=((jobID, reducersForThisJob))

      //log.info("[" + akka.serialization.Serialization.serializedActorPath(self) + "] ACKS#" + jobID + " reducers:" + reduceTasksRemaining.get(jobID).get.size)
    }
  }

  def deleteJobDetails(jobID:Int) ={
    reduceTasksRemaining.-=(jobID)
    reduceTaskResults.-=(jobID)
    mapTasksRemaining.-=(jobID)
  }
}